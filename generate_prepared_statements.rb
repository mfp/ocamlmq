#!/usr/bin/env ruby

funcname = ARGV.shift
stmts = ARGF.read.split(/;/).map{|x| x.strip}.reject{|x| x.empty?}

temp_stmts = stmts.map do |s|
  s = s.gsub(/create table/i, "create temporary table")
  %[PGSQL(dbh) "execute" "#{s.gsub(/"/, "\\\"")};"]
end

stmts = stmts.map{ |s| %[PGSQL(dbh) "#{s.gsub(/"/, "\\\"")};"] }

puts <<EOF
let #{funcname}_temp dbh =

#{temp_stmts.join(" >> \n")}

let #{funcname} dbh =

#{stmts.join(" >> \n")}
EOF
