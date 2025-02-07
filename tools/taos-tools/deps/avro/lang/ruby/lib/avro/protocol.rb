# frozen_string_literal: true
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
# https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

module Avro
  class Protocol
    VALID_TYPE_SCHEMA_TYPES = Set.new(%w[enum record error fixed])
    VALID_TYPE_SCHEMA_TYPES_SYM = Set.new(VALID_TYPE_SCHEMA_TYPES.map(&:to_sym))
    class ProtocolParseError < Avro::AvroError; end

    attr_reader :name, :namespace, :types, :messages, :md5, :doc
    def self.parse(protocol_string)
      json_data = MultiJson.load(protocol_string)

      if json_data.is_a? Hash
        name = json_data['protocol']
        namespace = json_data['namespace']
        types = json_data['types']
        messages = json_data['messages']
        doc = json_data['doc']
        Protocol.new(name, namespace, types, messages, doc)
      else
        raise ProtocolParseError, "Not a JSON object: #{json_data}"
      end
    end

    def initialize(name, namespace=nil, types=nil, messages=nil, doc=nil)
      # Ensure valid ctor args
      if !name
        raise ProtocolParseError, 'Protocols must have a non-empty name.'
      elsif !name.is_a?(String)
        raise ProtocolParseError, 'The name property must be a string.'
      elsif !namespace.is_a?(String)
        raise ProtocolParseError, 'The namespace property must be a string.'
      elsif !types.is_a?(Array)
        raise ProtocolParseError, 'The types property must be a list.'
      elsif !messages.is_a?(Hash)
        raise ProtocolParseError, 'The messages property must be a JSON object.'
      end

      @name = name
      @namespace = namespace
      type_names = {}
      @types = parse_types(types, type_names)
      @messages = parse_messages(messages, type_names)
      @md5 = Digest::MD5.digest(to_s)
      @doc = doc
    end

    def to_s
      MultiJson.dump to_avro
    end

    def ==(other)
      to_avro == other.to_avro
    end

    private
    def parse_types(types, type_names)
      types.collect do |type|
        # FIXME adding type.name to type_names is not defined in the
        # spec. Possible bug in the python impl and the spec.
        type_object = Schema.real_parse(type, type_names, namespace)
        unless VALID_TYPE_SCHEMA_TYPES_SYM.include?(type_object.type_sym)
          msg = "Type #{type} not an enum, record, fixed or error."
          raise ProtocolParseError, msg
        end
        type_object
      end
    end

    def parse_messages(messages, names)
      message_objects = {}
      messages.each do |name, body|
        if message_objects.has_key?(name)
          raise ProtocolParseError, "Message name \"#{name}\" repeated."
        elsif !body.is_a?(Hash)
          raise ProtocolParseError, "Message name \"#{name}\" has non-object body #{body.inspect}"
        end

        request  = body['request']
        response = body['response']
        errors   = body['errors']
        doc      = body['doc']
        message_objects[name] = Message.new(name, request, response, errors, names, namespace, doc)
      end
      message_objects
    end

    protected
    def to_avro(names=Set.new)
      hsh = {'protocol' => name}
      hsh['namespace'] = namespace if namespace
      hsh['types'] = types.map{|t| t.to_avro(names) } if types

      if messages
        hsh['messages'] = messages.inject({}) {|h, (k,t)| h[k] = t.to_avro(names); h }
      end

      hsh
    end

    class Message
      attr_reader :name, :request, :response, :errors, :default_namespace, :doc

      def initialize(name, request, response, errors=nil, names=nil, default_namespace=nil, doc=nil)
        @name = name
        @default_namespace = default_namespace
        @request = parse_request(request, names)
        @response = parse_response(response, names)
        @errors = parse_errors(errors, names) if errors
        @doc = doc
      end

      def to_avro(names=Set.new)
        {
          'request' => request.to_avro(names),
          'response' => response.to_avro(names)
        }.tap do |hash|
          hash['errors'] = errors.to_avro(names) if errors
          hash['doc'] = @doc if @doc
        end
      end

      def to_s
        Yajl.dump to_avro
      end

      def parse_request(request, names)
        unless request.is_a?(Array)
          raise ProtocolParseError, "Request property not an Array: #{request.inspect}"
        end
        Schema::RecordSchema.new(nil, default_namespace, request, names, :request)
      end

      def parse_response(response, names)
        if response.is_a?(String) && names
          fullname = Name.make_fullname(response, default_namespace)
          return names[fullname] if names.include?(fullname)
        end

        Schema.real_parse(response, names, default_namespace)
      end

      def parse_errors(errors, names)
        unless errors.is_a?(Array)
          raise ProtocolParseError, "Errors property not an Array: #{errors}"
        end
        Schema.real_parse(errors, names, default_namespace)
      end
    end
  end
end
