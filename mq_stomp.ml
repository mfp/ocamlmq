(* Copyright (c) 2010 Mauricio Fernández <mfp@acm.org> *)
open Mq_types
open Printf
open Lwt

module String = BatString

type stomp_frame = {
  fr_command : string;
  fr_headers : (string * string) list;
  fr_body : string;
}

let topic_re = Str.regexp "/topic/"
let queue_re = Str.regexp "/queue/"
let control_re = Str.regexp "/control/"

let get_destination frame =
  let destination = List.assoc "destination" frame.fr_headers in
    if Str.string_match topic_re destination 0 then
      Topic (String.slice ~first:7 destination)
    else if Str.string_match queue_re destination 0 then
      Queue (String.slice ~first:7 destination)
    else if Str.string_match control_re destination 0 then
      Control (String.slice ~first:9 destination)
    else raise Not_found

let get_header frame name = List.assoc (String.lowercase name) frame.fr_headers

let get_body frame = frame.fr_body

let stomp_frame_buf = Buffer.create 80

let write_stomp_frame ~eol och frame =
  let b = stomp_frame_buf in
    Buffer.clear b;
    Buffer.add_string b frame.fr_command;
    Buffer.add_char b '\n';
    List.iter
      (fun (k, v) -> if k <> "content-length" then begin
         Buffer.add_string b k;
         Buffer.add_string b ": ";
         Buffer.add_string b v;
         Buffer.add_char b '\n'
       end)
      frame.fr_headers;
    Buffer.add_string b "content-length: ";
    Buffer.add_string b (string_of_int (String.length frame.fr_body));
    Buffer.add_char b '\n';
    Buffer.add_char b '\n';
    let headers = Buffer.contents b in
      Lwt_io.atomic
        (fun och ->
           Lwt_io.write och headers >>
           Lwt_io.write och frame.fr_body >>
           (if eol then Lwt_io.write och "\000\n"
           else Lwt_io.write och "\000") >>
           Lwt_io.flush och)
        och

let handle_receipt ?(extra_headers=[]) ~eol och frame =
  try
    let receipt = List.assoc "receipt" frame.fr_headers in
      write_stomp_frame ~eol och
        { fr_command = "RECEIPT";
          fr_headers = ("receipt-id", receipt) :: extra_headers;
          fr_body = "" }
  with Not_found -> return ()

let send_message ~eol och msg =
  write_stomp_frame ~eol och
    {
      fr_command = "MESSAGE";
      fr_headers = [
        "message-id", msg.msg_id;
        "destination", string_of_destination msg.msg_destination;
        "content-length", string_of_int (String.length msg.msg_body);
      ];
      fr_body = msg.msg_body
    }

let send_error ~eol och fmt =
  ksprintf
    (fun msg ->
       write_stomp_frame ~eol och
         { fr_command = "ERROR"; fr_headers = []; fr_body = msg; })
    fmt

let read_stomp_headers ch =
  let rec loop acc =
    Lwt_io.read_line ch >>= function
        "" -> return acc
      | s ->
          match (try Some (String.split s ":") with _ -> None) with
              Some (k, v) -> loop ((String.lowercase k, String.strip v) :: acc)
            | None -> (* just ignore this line *) loop acc
  in loop []

let rec read_stomp_command ch =
  Lwt_io.read_line ch >>= function
      "" -> read_stomp_command ch
    | l -> return l

let read_until_zero ?(eol = true) ich =
  let b = Buffer.create 80 in
    if eol then begin
      let rec loop () =
        Lwt_io.read_line ich >>= function
            "" -> Buffer.add_char b '\n'; loop ()
          | l when l.[String.length l - 1] = '\000' ->
              Buffer.add_substring b l 0 (String.length l - 1);
              return (Buffer.contents b)
          | l -> Buffer.add_string b l;
                 Buffer.add_char b '\n';
                 loop ()
      in loop ()
    end else begin
      let rec loop () =
        Lwt_io.read_char ich >>= function
            '\000' -> return (Buffer.contents b)
          | c -> Buffer.add_char b c; loop ()
      in loop ()
    end


let read_stomp_frame ~eol ich =
  try_lwt
    lwt cmd = read_stomp_command ich in
    lwt headers = read_stomp_headers ich in
    lwt body =
      try
        let len = int_of_string (List.assoc "content-length" headers) in
        let body = String.create len in
        lwt () = Lwt_io.read_into_exactly ich body 0 len in
          lwt (_ : char) = Lwt_io.read_char ich in
            (* FIXME: check that it's \0 ? *)
            return body
      with Not_found -> read_until_zero ~eol ich
    in return { fr_command = cmd; fr_headers = headers; fr_body = body }
