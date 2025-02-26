" Vim syntax file
" Language: Avro IDL
" Maintainer: Daniel Lundin <dln@eintr.org>
" Last Change: 20100924
" Copy to ~/.vim/syntax/
" Add to ~/.vimrc
"  au BufRead,BufNewFile *.avdl setlocal filetype=avro-idl
"
" Licensed to the Apache Software Foundation (ASF) under one
" or more contributor license agreements. See the NOTICE file
" distributed with this work for additional information
" regarding copyright ownership. The ASF licenses this file
" to you under the Apache License, Version 2.0 (the
" "License"); you may not use this file except in compliance
" with the License. You may obtain a copy of the License at
"
"   https://www.apache.org/licenses/LICENSE-2.0
"
" Unless required by applicable law or agreed to in writing,
" software distributed under the License is distributed on an
" "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
" KIND, either express or implied. See the License for the
" specific language governing permissions and limitations
" under the License.
"

if version < 600
  syntax clear
elseif exists("b:current_syntax")
  finish
endif

" Todo
syn keyword avroTodo TODO todo FIXME fixme XXX xxx contained

" Comments
syn region avroComment start="/\*" end="\*/" contains=avroTodo
syn match avroComment "//.\{-}\(?>\|$\)\@=" contains=avroTodo

" Identifiers
syn region avroIdentifier start="^\s*\(error\|protocol\|record\)" end="{" contains=avroIdentifierType 
syn keyword avroIdentifierType error protocol record contained nextgroup=avroIdentifierName skipwhite
syn match avroIdentifierName	"\w\w*" display contained skipwhite

syn region avroEscaped  start=/`/ end=/`/

" Types
syn match avroNumber "-\=\<\d\+\>" contained
syn region avroString start=/"/ skip=/\\"/ end=/"/
syn region avroString start=/'/ skip=/\\'/ end=/'/
syn region avroArray  start="<" end=">" contains=avroArrayType
syn match avroArrayType "\w\w*" display contained skipwhite

" Keywords
syn keyword avroKeyword java-class namespace order
syn keyword avroKeyword error throws
syn keyword avroBasicTypes boolean bytes double fixed float int long null string void
syn keyword avroStructure array enum map union

if version >= 508 || !exists("did_avro_idl_syn_inits")
  if version < 508
    let did_avro_idl_syn_inits = 1
    command! -nargs=+ HiLink hi link <args>
  else
    command! -nargs=+ HiLink hi def link <args>
  endif

  HiLink avroTodo Todo
  HiLink avroComment Comment
  HiLink avroNumber Number
  HiLink avroKeyword Define
  HiLink avroIdentifierType Special
  HiLink avroBasicTypes Type
  HiLink avroArrayType Type
  HiLink avroString       String
  HiLink avroStructure Structure
  HiLink avroArray Structure
  HiLink avroEscaped Default
  HiLink avroIdentifierName    Entity

  delcommand HiLink
endif

let b:current_syntax = "avro-idl"
