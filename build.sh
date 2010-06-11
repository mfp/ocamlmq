#!/bin/sh

set -e

ocamlfind ocamlc -package lwt,lwt.unix,lwt.syntax,extlib,unix,str,pgocaml,pgocaml.syntax,camlp4.macro -warn-error A -syntax camlp4o -g -g -annot -I . -c mq_types.ml
ocamlfind ocamlopt -package lwt,lwt.unix,lwt.syntax,extlib,unix,str,pgocaml,pgocaml.syntax,camlp4.macro -warn-error A -syntax camlp4o -S -inline 100 -I . -c mq_types.ml
ocamlfind ocamlc -package lwt,lwt.unix,lwt.syntax,extlib,unix,str,pgocaml,pgocaml.syntax,camlp4.macro -warn-error A -syntax camlp4o -g -g -annot -I . -c pGOCaml_lwt.ml
ocamlfind ocamlopt -package lwt,lwt.unix,lwt.syntax,extlib,unix,str,pgocaml,pgocaml.syntax,camlp4.macro -warn-error A -syntax camlp4o -S -inline 100 -I . -c pGOCaml_lwt.ml
ocamlfind ocamlc -package lwt,lwt.unix,lwt.syntax,extlib,unix,str,pgocaml,pgocaml.syntax,camlp4.macro -warn-error A -syntax camlp4o -g -g -annot -I . -c extSet.mli
ocamlfind ocamlopt -package lwt,lwt.unix,lwt.syntax,extlib,unix,str,pgocaml,pgocaml.syntax,camlp4.macro -warn-error A -syntax camlp4o -S -inline 100 -I . -c extSet.ml
ocamlfind ocamlc -package lwt,lwt.unix,lwt.syntax,extlib,unix,str,pgocaml,pgocaml.syntax,camlp4.macro -warn-error A -syntax camlp4o -g -g -annot -I . -c ternary.mli
ocamlfind ocamlopt -package lwt,lwt.unix,lwt.syntax,extlib,unix,str,pgocaml,pgocaml.syntax,camlp4.macro -warn-error A -syntax camlp4o -S -inline 100 -I . -c ternary.ml
ruby generate_prepared_statements.rb create_db schema.sql > mq_schema.ml
ocamlfind ocamlc -package lwt,lwt.unix,lwt.syntax,extlib,unix,str,pgocaml,pgocaml.syntax,camlp4.macro -warn-error A -syntax camlp4o -g -g -annot -I . -c mq_stomp.ml
ocamlfind ocamlopt -package lwt,lwt.unix,lwt.syntax,extlib,unix,str,pgocaml,pgocaml.syntax,camlp4.macro -warn-error A -syntax camlp4o -S -inline 100 -I . -c mq_stomp.ml
ocamlfind ocamlc -package lwt,lwt.unix,lwt.syntax,extlib,unix,str,pgocaml,pgocaml.syntax,camlp4.macro -warn-error A -syntax camlp4o -g -g -annot -I . -c mq_server.ml
ocamlfind ocamlopt -package lwt,lwt.unix,lwt.syntax,extlib,unix,str,pgocaml,pgocaml.syntax,camlp4.macro -warn-error A -syntax camlp4o -S -inline 100 -I . -c mq_server.ml
ocamlfind ocamlc -package lwt,lwt.unix,lwt.syntax,extlib,unix,str,pgocaml,pgocaml.syntax,camlp4.macro -warn-error A -syntax camlp4o -g -g -annot -I . -c mq_pg_persistence.mli
ocamlfind ocamlopt -package lwt,lwt.unix,lwt.syntax,extlib,unix,str,pgocaml,pgocaml.syntax,camlp4.macro -warn-error A -syntax camlp4o -S -inline 100 -I . -c mq_pg_persistence.ml
ocamlfind ocamlc -package lwt,lwt.unix,lwt.syntax,extlib,unix,str,pgocaml,pgocaml.syntax,camlp4.macro -warn-error A -syntax camlp4o -g -g -annot -I . -c ocamlmq.ml
ocamlfind ocamlopt -package lwt,lwt.unix,lwt.syntax,extlib,unix,str,pgocaml,pgocaml.syntax,camlp4.macro -warn-error A -syntax camlp4o -S -inline 100 -I . -c ocamlmq.ml
ocamlfind ocamlopt -package lwt,lwt.unix,lwt.syntax,extlib,unix,str,pgocaml,pgocaml.syntax,camlp4.macro -warn-error A -syntax camlp4o -S -inline 100 -I . -o ocamlmq extSet.cmx mq_types.cmx mq_stomp.cmx pGOCaml_lwt.cmx mq_pg_persistence.cmx ternary.cmx mq_server.cmx ocamlmq.cmx -linkpkg
