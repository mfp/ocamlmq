#!/bin/sh

set -e
set -x

cd ocaml-sqlexpr

ocamlfind ocamlc -syntax camlp4o -package estring,camlp4.quotations -warn-error A -g -I . -c pa_sql.ml
ocamlfind ocamlc -syntax camlp4o -syntax camlp4o -ppopt pa_sql.cmo -package csv,extlib,sqlite3,estring,lwt,lwt.syntax -warn-error A -g -I . -c sqlexpr_concurrency.ml
ocamlfind ocamlopt -syntax camlp4o -syntax camlp4o -ppopt pa_sql.cmo -package csv,extlib,sqlite3,estring,lwt,lwt.syntax -warn-error A -S -inline 100 -I . -c sqlexpr_concurrency.ml
ocamlfind ocamlc -syntax camlp4o -syntax camlp4o -ppopt pa_sql.cmo -package csv,extlib,sqlite3,estring,lwt,lwt.syntax -warn-error A -g -I . -c sqlexpr_sqlite.mli
ocamlfind ocamlopt -syntax camlp4o -syntax camlp4o -ppopt pa_sql.cmo -package csv,extlib,sqlite3,estring,lwt,lwt.syntax -warn-error A -S -inline 100 -I . -c sqlexpr_sqlite.ml
ocamlfind ocamlc -syntax camlp4o -syntax camlp4o -ppopt pa_sql.cmo -package csv,extlib,sqlite3,estring,lwt,lwt.syntax -warn-error A -g -I . -c sqlexpr_sqlite.ml
ocamlfind ocamlopt -syntax camlp4o -syntax camlp4o -ppopt pa_sql.cmo -package csv,extlib,sqlite3,estring,lwt,lwt.syntax -warn-error A -S -inline 100 -a -o sqlexpr.cmxa sqlexpr_concurrency.cmx sqlexpr_sqlite.cmx
ocamlfind ocamlc -syntax camlp4o -syntax camlp4o -ppopt pa_sql.cmo -package csv,extlib,sqlite3,estring,lwt,lwt.syntax -warn-error A -g -a -o sqlexpr.cma sqlexpr_concurrency.cmo sqlexpr_sqlite.cmo

cd ..

ocamlfind ocamlc -syntax camlp4o -package csv,lwt,lwt.unix,lwt.syntax,estring,extlib,unix,str,sqlite3,camlp4.macro -warn-error A -g -g -annot -I . -I ocaml-sqlexpr -c mq_types.ml
ocamlfind ocamlopt -syntax camlp4o -package csv,lwt,lwt.unix,lwt.syntax,estring,extlib,unix,str,sqlite3,camlp4.macro -warn-error A -S -inline 100 -I . -I ocaml-sqlexpr -c mq_types.ml
ocamlfind ocamlc -syntax camlp4o -package csv,lwt,lwt.unix,lwt.syntax,estring,extlib,unix,str,sqlite3,camlp4.macro -warn-error A -g -g -annot -I . -I ocaml-sqlexpr -c binlog.mli
ocamlfind ocamlopt -syntax camlp4o -package csv,lwt,lwt.unix,lwt.syntax,estring,extlib,unix,str,sqlite3,camlp4.macro -warn-error A -S -inline 100 -I . -I ocaml-sqlexpr -c binlog.ml
ocamlfind ocamlc -syntax camlp4o -package csv,lwt,lwt.unix,lwt.syntax,estring,extlib,unix,str,sqlite3,camlp4.macro -warn-error A -g -g -annot -I . -I ocaml-sqlexpr -c extSet.mli
ocamlfind ocamlopt -syntax camlp4o -package csv,lwt,lwt.unix,lwt.syntax,estring,extlib,unix,str,sqlite3,camlp4.macro -warn-error A -S -inline 100 -I . -I ocaml-sqlexpr -c extSet.ml
ocamlfind ocamlc -syntax camlp4o -package csv,lwt,lwt.unix,lwt.syntax,estring,extlib,unix,str,sqlite3,camlp4.macro -warn-error A -g -g -annot -I . -I ocaml-sqlexpr -c ternary.mli
ocamlfind ocamlopt -syntax camlp4o -package csv,lwt,lwt.unix,lwt.syntax,estring,extlib,unix,str,sqlite3,camlp4.macro -warn-error A -S -inline 100 -I . -I ocaml-sqlexpr -c ternary.ml
ocamlfind ocamlc -syntax camlp4o -package csv,lwt,lwt.unix,lwt.syntax,estring,extlib,unix,str,sqlite3,camlp4.macro -warn-error A -g -g -annot -I . -I ocaml-sqlexpr -c mq_stomp.ml
ocamlfind ocamlopt -syntax camlp4o -package csv,lwt,lwt.unix,lwt.syntax,estring,extlib,unix,str,sqlite3,camlp4.macro -warn-error A -S -inline 100 -I . -I ocaml-sqlexpr -c mq_stomp.ml
ocamlfind ocamlc -syntax camlp4o -package csv,lwt,lwt.unix,lwt.syntax,estring,extlib,unix,str,sqlite3,camlp4.macro -warn-error A -g -g -annot -I . -I ocaml-sqlexpr -c mq_server.ml
ocamlfind ocamlopt -syntax camlp4o -package csv,lwt,lwt.unix,lwt.syntax,estring,extlib,unix,str,sqlite3,camlp4.macro -warn-error A -S -inline 100 -I . -I ocaml-sqlexpr -c mq_server.ml
ocamlfind ocamlc -syntax camlp4o -ppopt ocaml-sqlexpr/pa_sql.cmo -package csv,lwt,lwt.unix,lwt.syntax,estring,extlib,unix,str,sqlite3,camlp4.macro -warn-error A -g -g -annot -I . -I ocaml-sqlexpr -c mq_sqlite_persistence.mli
ocamlfind ocamlopt -syntax camlp4o -ppopt ocaml-sqlexpr/pa_sql.cmo -package csv,lwt,lwt.unix,lwt.syntax,estring,extlib,unix,str,sqlite3,camlp4.macro -warn-error A -S -inline 100 -I . -I ocaml-sqlexpr -c mq_sqlite_persistence.ml
ocamlfind ocamlopt -syntax camlp4o -ppopt ocaml-sqlexpr/pa_sql.cmo -package csv,lwt,lwt.unix,lwt.syntax,estring,extlib,unix,str,sqlite3,camlp4.macro -warn-error A -S -inline 100 -I . -I ocaml-sqlexpr -c mq_sqlite_persistence.ml
ocamlfind ocamlc -syntax camlp4o -package csv,lwt,lwt.unix,lwt.syntax,estring,extlib,unix,str,sqlite3,camlp4.macro -warn-error A -g -g -annot -I . -I ocaml-sqlexpr -c ocamlmq.ml
ocamlfind ocamlopt -syntax camlp4o -package csv,lwt,lwt.unix,lwt.syntax,estring,extlib,unix,str,sqlite3,camlp4.macro -warn-error A -S -inline 100 -I . -I ocaml-sqlexpr -c ocamlmq.ml
ocamlfind ocamlopt -syntax camlp4o -package csv,lwt,lwt.unix,lwt.syntax,estring,extlib,unix,str,sqlite3,camlp4.macro -warn-error A -S -inline 100 -I . -I ocaml-sqlexpr -o ocamlmq ocaml-sqlexpr/sqlexpr.cmxa mq_types.cmx binlog.cmx extSet.cmx mq_stomp.cmx ternary.cmx mq_server.cmx mq_sqlite_persistence.cmx ocamlmq.cmx -linkpkg

