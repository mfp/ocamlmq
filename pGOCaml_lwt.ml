(* Copyright (c) 2010 Mauricio Fernández <mfp@acm.org> *)
include PGOCaml_generic.Make(struct include Lwt include Lwt_chan end)

let buf = Buffer.create 64000

let string_of_bytea b =
  Buffer.clear buf;
  let len = String.length b in
  for i = 0 to len - 1 do
    let c = String.unsafe_get b i in
    let cc = Char.code c in
    if cc < 0x20 || cc > 0x7e then begin
      Buffer.add_char buf '\\';
      Buffer.add_char buf (Char.unsafe_chr (48 + ((cc lsr 6) land 0x7))); (* '0' + higher bits *)
      Buffer.add_char buf (Char.unsafe_chr (48 + ((cc lsr 3) land 0x7)));
      Buffer.add_char buf (Char.unsafe_chr (48 + (cc land 0x7)));
      (* Buffer.add_string buf (sprintf "\\%03o" cc) [> non-print -> \ooo <] *)
    end else if c = '\\' then
      Buffer.add_string buf "\\\\" (* \ -> \\ *)
    else
      Buffer.add_char buf c
  done;
  Buffer.contents buf
