
type destination = Queue of string | Topic of string

type message = {
  msg_id : string;
  msg_destination : destination;
  msg_priority : int;
  msg_timestamp : float;
  msg_body : string;
  msg_ack_timeout : float;
}

let string_of_destination = function
    Topic n -> "/topic/" ^ n
  | Queue n -> "/queue/" ^ n

let destination_name = function Topic n | Queue n -> n
