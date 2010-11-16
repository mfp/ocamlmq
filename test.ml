open OUnit

let test_sql f () =
  let b = Buffer.create 13 in
  let fmt = Format.formatter_of_buffer b in
  let contents b = Format.fprintf fmt "@?"; Buffer.contents b in
    if not (f fmt) then
      assert_failure (contents b)

let db_tests =
  [
    "Mq_sqlite_persistence", Mq_sqlite_persistence.auto_check_db;
  ]

let all_tests =
  List.map (fun (n, f) -> n >:: test_sql f) db_tests @
  [
  ]


let _ =
  run_test_tt_main ("All" >::: all_tests)
