#!/usr/bin/env ruby

funcname = ARGV.shift
stmts = ARGF.read.split(/;/).map{|x| x.strip}.reject{|x| x.empty?}

temp_stmts = stmts.map do |s|
  s = s.gsub(/create table/i, "create temporary table")
  %[PGSQL(dbh) "execute" "#{s.gsub(/"/, "\\\"")};"]
end

puts <<EOF
let #{funcname} dbh =

#{temp_stmts.join(";\n")}
EOF
