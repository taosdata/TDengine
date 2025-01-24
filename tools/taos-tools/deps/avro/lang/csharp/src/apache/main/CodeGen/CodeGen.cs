/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
using System;
using System.CodeDom;
using System.CodeDom.Compiler;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Reflection;
using System.Text;
using Microsoft.CSharp;

namespace Avro
{
    /// <summary>
    /// Generates C# code from Avro schemas and protocols.
    /// </summary>
    public class CodeGen
    {
        /// <summary>
        /// Object that contains all the generated types
        /// </summary>
        public CodeCompileUnit CompileUnit { get; private set; }

        /// <summary>
        /// List of schemas to generate code for
        /// </summary>
        public IList<Schema> Schemas { get; private set; }

        /// <summary>
        /// List of protocols to generate code for
        /// </summary>
        public IList<Protocol> Protocols { get; private set; }

        /// <summary>
        /// Mapping of Avro namespaces to C# namespaces
        /// </summary>
        public IDictionary<string, string> NamespaceMapping { get; private set; }

        /// <summary>
        /// List of generated namespaces
        /// </summary>
        [Obsolete("Use NamespaceLookup instead. This will be removed from the public API in a future version.")]
        protected Dictionary<string, CodeNamespace> namespaceLookup = new Dictionary<string, CodeNamespace>(StringComparer.Ordinal);

        /// <summary>
        /// List of generated namespaces.
        /// </summary>
        protected Dictionary<string, CodeNamespace> NamespaceLookup
        {
#pragma warning disable CS0618 // Type or member is obsolete
            get => namespaceLookup;
            set => namespaceLookup = value;
#pragma warning restore CS0618 // Type or member is obsolete
        }

        /// <summary>
        /// Default constructor
        /// </summary>
        public CodeGen()
        {
            this.Schemas = new List<Schema>();
            this.Protocols = new List<Protocol>();
            this.NamespaceMapping = new Dictionary<string, string>();
        }

        /// <summary>
        /// Adds a protocol object to generate code for
        /// </summary>
        /// <param name="protocol">protocol object</param>
        public virtual void AddProtocol(Protocol protocol)
        {
            Protocols.Add(protocol);
        }

        /// <summary>
        /// Adds a schema object to generate code for
        /// </summary>
        /// <param name="schema">schema object</param>
        public virtual void AddSchema(Schema schema)
        {
            Schemas.Add(schema);
        }

        /// <summary>
        /// Adds a namespace object for the given name into the dictionary if it doesn't exist yet
        /// </summary>
        /// <param name="name">name of namespace</param>
        /// <returns></returns>
        protected virtual CodeNamespace addNamespace(string name)
        {
            if (string.IsNullOrEmpty(name))
                throw new ArgumentNullException(nameof(name), "name cannot be null.");

            CodeNamespace ns = null;

            if (!NamespaceLookup.TryGetValue(name, out ns))
            {
                string csharpNamespace;
                ns = NamespaceMapping.TryGetValue(name, out csharpNamespace)
                    ? new CodeNamespace(csharpNamespace)
                    : new CodeNamespace(CodeGenUtil.Instance.Mangle(name));

                foreach (CodeNamespaceImport nci in CodeGenUtil.Instance.NamespaceImports)
                    ns.Imports.Add(nci);

                CompileUnit.Namespaces.Add(ns);
                NamespaceLookup.Add(name, ns);
            }
            return ns;
        }

        /// <summary>
        /// Generates code for the given protocol and schema objects
        /// </summary>
        /// <returns>CodeCompileUnit object</returns>
        public virtual CodeCompileUnit GenerateCode()
        {
            CompileUnit = new CodeCompileUnit();

            processSchemas();
            processProtocols();

            return CompileUnit;
        }

        /// <summary>
        /// Generates code for the schema objects
        /// </summary>
        protected virtual void processSchemas()
        {
            foreach (Schema schema in this.Schemas)
            {
                SchemaNames names = generateNames(schema);
                foreach (KeyValuePair<SchemaName, NamedSchema> sn in names)
                {
                    switch (sn.Value.Tag)
                    {
                        case Schema.Type.Enumeration: processEnum(sn.Value); break;
                        case Schema.Type.Fixed: processFixed(sn.Value); break;
                        case Schema.Type.Record: processRecord(sn.Value); break;
                        case Schema.Type.Error: processRecord(sn.Value); break;
                        default:
                            throw new CodeGenException("Names in schema should only be of type NamedSchema, type found " + sn.Value.Tag);
                    }
                }
            }
        }

        /// <summary>
        /// Generates code for the protocol objects
        /// </summary>
        protected virtual void processProtocols()
        {
            foreach (Protocol protocol in Protocols)
            {
                SchemaNames names = generateNames(protocol);
                foreach (KeyValuePair<SchemaName, NamedSchema> sn in names)
                {
                    switch (sn.Value.Tag)
                    {
                        case Schema.Type.Enumeration: processEnum(sn.Value); break;
                        case Schema.Type.Fixed: processFixed(sn.Value); break;
                        case Schema.Type.Record: processRecord(sn.Value); break;
                        case Schema.Type.Error: processRecord(sn.Value); break;
                        default:
                            throw new CodeGenException("Names in protocol should only be of type NamedSchema, type found " + sn.Value.Tag);
                    }
                }

                processInterface(protocol);
            }
        }

        /// <summary>
        /// Generate list of named schemas from given protocol
        /// </summary>
        /// <param name="protocol">protocol to process</param>
        /// <returns></returns>
        protected virtual SchemaNames generateNames(Protocol protocol)
        {
            var names = new SchemaNames();
            foreach (Schema schema in protocol.Types)
                addName(schema, names);
            return names;
        }

        /// <summary>
        /// Generate list of named schemas from given schema
        /// </summary>
        /// <param name="schema">schema to process</param>
        /// <returns></returns>
        protected virtual SchemaNames generateNames(Schema schema)
        {
            var names = new SchemaNames();
            addName(schema, names);
            return names;
        }

        /// <summary>
        /// Recursively search the given schema for named schemas and adds them to the given container
        /// </summary>
        /// <param name="schema">schema object to search</param>
        /// <param name="names">list of named schemas</param>
        protected virtual void addName(Schema schema, SchemaNames names)
        {
            NamedSchema ns = schema as NamedSchema;
            if (null != ns) if (names.Contains(ns.SchemaName)) return;

            switch (schema.Tag)
            {
                case Schema.Type.Null:
                case Schema.Type.Boolean:
                case Schema.Type.Int:
                case Schema.Type.Long:
                case Schema.Type.Float:
                case Schema.Type.Double:
                case Schema.Type.Bytes:
                case Schema.Type.String:
                case Schema.Type.Logical:
                    break;

                case Schema.Type.Enumeration:
                case Schema.Type.Fixed:
                    names.Add(ns);
                    break;

                case Schema.Type.Record:
                case Schema.Type.Error:
                    var rs = schema as RecordSchema;
                    names.Add(rs);
                    foreach (Field field in rs.Fields)
                        addName(field.Schema, names);
                    break;

                case Schema.Type.Array:
                    var asc = schema as ArraySchema;
                    addName(asc.ItemSchema, names);
                    break;

                case Schema.Type.Map:
                    var ms = schema as MapSchema;
                    addName(ms.ValueSchema, names);
                    break;

                case Schema.Type.Union:
                    var us = schema as UnionSchema;
                    foreach (Schema usc in us.Schemas)
                        addName(usc, names);
                    break;

                default:
                    throw new CodeGenException("Unable to add name for " + schema.Name + " type " + schema.Tag);
            }
        }

        /// <summary>
        /// Creates a class declaration for fixed schema
        /// </summary>
        /// <param name="schema">fixed schema</param>
        protected virtual void processFixed(Schema schema)
        {
            FixedSchema fixedSchema = schema as FixedSchema;
            if (null == fixedSchema) throw new CodeGenException("Unable to cast schema into a fixed");

            CodeTypeDeclaration ctd = new CodeTypeDeclaration();
            ctd.Name = CodeGenUtil.Instance.Mangle(fixedSchema.Name);
            ctd.IsClass = true;
            ctd.IsPartial = true;
            ctd.Attributes = MemberAttributes.Public;
            ctd.BaseTypes.Add("SpecificFixed");

            if (fixedSchema.Documentation != null)
            {
                ctd.Comments.Add(createDocComment(fixedSchema.Documentation));
            }

            // create static schema field
            createSchemaField(schema, ctd, true);

            // Add Size field
            string sizefname = "fixedSize";
            var ctrfield = new CodeTypeReference(typeof(uint));
            var codeField = new CodeMemberField(ctrfield, sizefname);
            codeField.Attributes = MemberAttributes.Private | MemberAttributes.Static;
            codeField.InitExpression = new CodePrimitiveExpression(fixedSchema.Size);
            ctd.Members.Add(codeField);

            // Add Size property
            var property = new CodeMemberProperty();
            property.Attributes = MemberAttributes.Public | MemberAttributes.Static;
            property.Name = "FixedSize";
            property.Type = ctrfield;
            property.GetStatements.Add(new CodeMethodReturnStatement(new CodeTypeReferenceExpression(schema.Name + "." + sizefname)));
            ctd.Members.Add(property);

            // create constructor to initiate base class SpecificFixed
            CodeConstructor cc = new CodeConstructor();
            cc.Attributes = MemberAttributes.Public;
            cc.BaseConstructorArgs.Add(new CodeVariableReferenceExpression(sizefname));
            ctd.Members.Add(cc);

            string nspace = fixedSchema.Namespace;
            if (string.IsNullOrEmpty(nspace))
                throw new CodeGenException("Namespace required for enum schema " + fixedSchema.Name);
            CodeNamespace codens = addNamespace(nspace);
            codens.Types.Add(ctd);
        }

        /// <summary>
        /// Creates an enum declaration
        /// </summary>
        /// <param name="schema">enum schema</param>
        protected virtual void processEnum(Schema schema)
        {
            EnumSchema enumschema = schema as EnumSchema;
            if (null == enumschema) throw new CodeGenException("Unable to cast schema into an enum");

            CodeTypeDeclaration ctd = new CodeTypeDeclaration(CodeGenUtil.Instance.Mangle(enumschema.Name));
            ctd.IsEnum = true;
            ctd.Attributes = MemberAttributes.Public;

            if (enumschema.Documentation != null)
            {
                ctd.Comments.Add(createDocComment(enumschema.Documentation));
            }

            foreach (string symbol in enumschema.Symbols)
            {
                if (CodeGenUtil.Instance.ReservedKeywords.Contains(symbol))
                    throw new CodeGenException("Enum symbol " + symbol + " is a C# reserved keyword");
                CodeMemberField field = new CodeMemberField(typeof(int), symbol);
                ctd.Members.Add(field);
            }

            string nspace = enumschema.Namespace;
            if (string.IsNullOrEmpty(nspace))
                throw new CodeGenException("Namespace required for enum schema " + enumschema.Name);
            CodeNamespace codens = addNamespace(nspace);

            codens.Types.Add(ctd);
        }

        /// <summary>
        /// Generates code for an individual protocol.
        /// </summary>
        /// <param name="protocol">Protocol to generate code for.</param>
        protected virtual void processInterface(Protocol protocol)
        {
            // Create abstract class
            string protocolNameMangled = CodeGenUtil.Instance.Mangle(protocol.Name);

            var ctd = new CodeTypeDeclaration(protocolNameMangled);
            ctd.TypeAttributes = TypeAttributes.Abstract | TypeAttributes.Public;
            ctd.IsClass = true;
            ctd.BaseTypes.Add("Avro.Specific.ISpecificProtocol");

            AddProtocolDocumentation(protocol, ctd);

            // Add static protocol field.
            var protocolField = new CodeMemberField();
            protocolField.Attributes = MemberAttributes.Private | MemberAttributes.Static | MemberAttributes.Final;
            protocolField.Name = "protocol";
            protocolField.Type = new CodeTypeReference("readonly Avro.Protocol");

            var cpe = new CodePrimitiveExpression(protocol.ToString());
            var cmie = new CodeMethodInvokeExpression(
                new CodeMethodReferenceExpression(new CodeTypeReferenceExpression(typeof(Protocol)), "Parse"),
                new CodeExpression[] { cpe });

            protocolField.InitExpression = cmie;

            ctd.Members.Add(protocolField);

            // Add overridden Protocol method.
            var property = new CodeMemberProperty();
            property.Attributes = MemberAttributes.Public | MemberAttributes.Final;
            property.Name = "Protocol";
            property.Type = new CodeTypeReference("Avro.Protocol");
            property.HasGet = true;


            property.GetStatements.Add(new CodeTypeReferenceExpression("return protocol"));
            ctd.Members.Add(property);

            //var requestMethod = CreateRequestMethod();
            //ctd.Members.Add(requestMethod);

            var requestMethod = CreateRequestMethod();
            //requestMethod.Attributes |= MemberAttributes.Override;
            var builder = new StringBuilder();

            if (protocol.Messages.Count > 0)
            {
                builder.AppendLine("switch(messageName)");
                builder.Append("\t\t\t{");

                foreach (var a in protocol.Messages)
                {
                    builder.AppendLine().Append("\t\t\t\tcase \"").Append(a.Key).AppendLine("\":");

                    bool unused = false;
                    string type = getType(a.Value.Response, false, ref unused);

                    builder.Append("\t\t\t\trequestor.Request<")
                           .Append(type)
                           .AppendLine(">(messageName, args, callback);");
                    builder.AppendLine("\t\t\t\tbreak;");
                }

                builder.Append("\t\t\t}");
            }
            var cseGet = new CodeSnippetExpression(builder.ToString());

            requestMethod.Statements.Add(cseGet);
            ctd.Members.Add(requestMethod);

            AddMethods(protocol, false, ctd);

            string nspace = protocol.Namespace;
            if (string.IsNullOrEmpty(nspace))
                throw new CodeGenException("Namespace required for enum schema " + nspace);
            CodeNamespace codens = addNamespace(nspace);

            codens.Types.Add(ctd);

            // Create callback abstract class
            ctd = new CodeTypeDeclaration(protocolNameMangled + "Callback");
            ctd.TypeAttributes = TypeAttributes.Abstract | TypeAttributes.Public;
            ctd.IsClass = true;
            ctd.BaseTypes.Add(protocolNameMangled);

            // Need to override



            AddProtocolDocumentation(protocol, ctd);

            AddMethods(protocol, true, ctd);

            codens.Types.Add(ctd);
        }

        private static CodeMemberMethod CreateRequestMethod()
        {
            var requestMethod = new CodeMemberMethod();
            requestMethod.Attributes = MemberAttributes.Public | MemberAttributes.Final;
            requestMethod.Name = "Request";
            requestMethod.ReturnType = new CodeTypeReference(typeof (void));
            {
                var requestor = new CodeParameterDeclarationExpression(typeof (Avro.Specific.ICallbackRequestor),
                                                                       "requestor");
                requestMethod.Parameters.Add(requestor);

                var messageName = new CodeParameterDeclarationExpression(typeof (string), "messageName");
                requestMethod.Parameters.Add(messageName);

                var args = new CodeParameterDeclarationExpression(typeof (object[]), "args");
                requestMethod.Parameters.Add(args);

                var callback = new CodeParameterDeclarationExpression(typeof (object), "callback");
                requestMethod.Parameters.Add(callback);
            }
            return requestMethod;
        }

        private static void AddMethods(Protocol protocol, bool generateCallback, CodeTypeDeclaration ctd)
        {
            foreach (var e in protocol.Messages)
            {
                var name = e.Key;
                var message = e.Value;
                var response = message.Response;

                if (generateCallback && message.Oneway.GetValueOrDefault())
                    continue;

                var messageMember = new CodeMemberMethod();
                messageMember.Name = CodeGenUtil.Instance.Mangle(name);
                messageMember.Attributes = MemberAttributes.Public | MemberAttributes.Abstract;

                if (message.Doc!= null && message.Doc.Trim() != string.Empty)
                    messageMember.Comments.Add(new CodeCommentStatement(message.Doc));

                if (message.Oneway.GetValueOrDefault() || generateCallback)
                {
                    messageMember.ReturnType = new CodeTypeReference(typeof (void));
                }
                else
                {
                    bool ignored = false;
                    string type = getType(response, false, ref ignored);

                    messageMember.ReturnType = new CodeTypeReference(type);
                }

                foreach (Field field in message.Request.Fields)
                {
                    bool ignored = false;
                    string type = getType(field.Schema, false, ref ignored);

                    string fieldName = CodeGenUtil.Instance.Mangle(field.Name);
                    var parameter = new CodeParameterDeclarationExpression(type, fieldName);
                    messageMember.Parameters.Add(parameter);
                }

                if (generateCallback)
                {
                    bool unused = false;
                    var type = getType(response, false, ref unused);
                    var parameter = new CodeParameterDeclarationExpression("Avro.IO.ICallback<" + type + ">",
                                                                           "callback");
                    messageMember.Parameters.Add(parameter);
                }


                ctd.Members.Add(messageMember);
            }
        }

        private void AddProtocolDocumentation(Protocol protocol, CodeTypeDeclaration ctd)
        {
            // Add interface documentation
            if (protocol.Doc != null && protocol.Doc.Trim() != string.Empty)
            {
                var interfaceDoc = createDocComment(protocol.Doc);
                if (interfaceDoc != null)
                    ctd.Comments.Add(interfaceDoc);
            }
        }

        /// <summary>
        /// Creates a class declaration
        /// </summary>
        /// <param name="schema">record schema</param>
        /// <returns>A new class code type declaration</returns>
        protected virtual CodeTypeDeclaration processRecord(Schema schema)
        {
            RecordSchema recordSchema = schema as RecordSchema;
            if (null == recordSchema) throw new CodeGenException("Unable to cast schema into a record");

            bool isError = recordSchema.Tag == Schema.Type.Error;

            // declare the class
            var ctd = new CodeTypeDeclaration(CodeGenUtil.Instance.Mangle(recordSchema.Name));
            ctd.BaseTypes.Add(isError ? "SpecificException" : "ISpecificRecord");

            ctd.Attributes = MemberAttributes.Public;
            ctd.IsClass = true;
            ctd.IsPartial = true;

            if (recordSchema.Documentation != null)
            {
                ctd.Comments.Add(createDocComment(recordSchema.Documentation));
            }

            createSchemaField(schema, ctd, isError);

            // declare Get() to be used by the Writer classes
            var cmmGet = new CodeMemberMethod();
            cmmGet.Name = "Get";
            cmmGet.Attributes = MemberAttributes.Public;
            cmmGet.ReturnType = new CodeTypeReference("System.Object");
            cmmGet.Parameters.Add(new CodeParameterDeclarationExpression(typeof(int), "fieldPos"));
            StringBuilder getFieldStmt = new StringBuilder("switch (fieldPos)")
                .AppendLine().AppendLine("\t\t\t{");

            // declare Put() to be used by the Reader classes
            var cmmPut = new CodeMemberMethod();
            cmmPut.Name = "Put";
            cmmPut.Attributes = MemberAttributes.Public;
            cmmPut.ReturnType = new CodeTypeReference(typeof(void));
            cmmPut.Parameters.Add(new CodeParameterDeclarationExpression(typeof(int), "fieldPos"));
            cmmPut.Parameters.Add(new CodeParameterDeclarationExpression("System.Object", "fieldValue"));
            var putFieldStmt = new StringBuilder("switch (fieldPos)")
                .AppendLine().AppendLine("\t\t\t{");

            if (isError)
            {
                cmmGet.Attributes |= MemberAttributes.Override;
                cmmPut.Attributes |= MemberAttributes.Override;
            }

            foreach (Field field in recordSchema.Fields)
            {
                // Determine type of field
                bool nullibleEnum = false;
                string baseType = getType(field.Schema, false, ref nullibleEnum);
                var ctrfield = new CodeTypeReference(baseType);

                // Create field
                string privFieldName = string.Concat("_", field.Name);
                var codeField = new CodeMemberField(ctrfield, privFieldName);
                codeField.Attributes = MemberAttributes.Private;
                if (field.Schema is EnumSchema es && es.Default != null)
                {
                    codeField.InitExpression = new CodeTypeReferenceExpression($"{es.Name}.{es.Default}");
                }

                // Process field documentation if it exist and add to the field
                CodeCommentStatement propertyComment = null;
                if (!string.IsNullOrEmpty(field.Documentation))
                {
                    propertyComment = createDocComment(field.Documentation);
                    if (null != propertyComment)
                        codeField.Comments.Add(propertyComment);
                }

                // Add field to class
                ctd.Members.Add(codeField);

                // Create reference to the field - this.fieldname
                var fieldRef = new CodeFieldReferenceExpression(new CodeThisReferenceExpression(), privFieldName);
                var mangledName = CodeGenUtil.Instance.Mangle(field.Name);

                // Create field property with get and set methods
                var property = new CodeMemberProperty();
                property.Attributes = MemberAttributes.Public | MemberAttributes.Final;
                property.Name = mangledName;
                property.Type = ctrfield;
                property.GetStatements.Add(new CodeMethodReturnStatement(fieldRef));
                property.SetStatements.Add(new CodeAssignStatement(fieldRef, new CodePropertySetValueReferenceExpression()));
                if (null != propertyComment)
                    property.Comments.Add(propertyComment);

                // Add field property to class
                ctd.Members.Add(property);

                // add to Get()
                getFieldStmt.Append("\t\t\tcase ");
                getFieldStmt.Append(field.Pos);
                getFieldStmt.Append(": return this.");
                getFieldStmt.Append(mangledName);
                getFieldStmt.AppendLine(";");

                // add to Put()
                putFieldStmt.Append("\t\t\tcase ");
                putFieldStmt.Append(field.Pos);
                putFieldStmt.Append(": this.");
                putFieldStmt.Append(mangledName);

                if (nullibleEnum)
                {
                    putFieldStmt.Append(" = fieldValue == null ? (");
                    putFieldStmt.Append(baseType);
                    putFieldStmt.Append(")null : (");

                    string type = baseType.Remove(0, 16);  // remove System.Nullable<
                    type = type.Remove(type.Length - 1);   // remove >

                    putFieldStmt.Append(type);
                    putFieldStmt.AppendLine(")fieldValue; break;");
                }
                else
                {
                    putFieldStmt.Append(" = (");
                    putFieldStmt.Append(baseType);
                    putFieldStmt.AppendLine(")fieldValue; break;");
                }
            }

            // end switch block for Get()
            getFieldStmt.AppendLine("\t\t\tdefault: throw new AvroRuntimeException(\"Bad index \" + fieldPos + \" in Get()\");")
                .Append("\t\t\t}");
            var cseGet = new CodeSnippetExpression(getFieldStmt.ToString());
            cmmGet.Statements.Add(cseGet);
            ctd.Members.Add(cmmGet);

            // end switch block for Put()
            putFieldStmt.AppendLine("\t\t\tdefault: throw new AvroRuntimeException(\"Bad index \" + fieldPos + \" in Put()\");")
                .Append("\t\t\t}");
            var csePut = new CodeSnippetExpression(putFieldStmt.ToString());
            cmmPut.Statements.Add(csePut);
            ctd.Members.Add(cmmPut);

            string nspace = recordSchema.Namespace;
            if (string.IsNullOrEmpty(nspace))
                throw new CodeGenException("Namespace required for record schema " + recordSchema.Name);
            CodeNamespace codens = addNamespace(nspace);

            codens.Types.Add(ctd);

            return ctd;
        }

        /// <summary>
        /// Gets the string representation of the schema's data type
        /// </summary>
        /// <param name="schema">schema</param>
        /// <param name="nullible">flag to indicate union with null</param>
        /// <param name="nullibleEnum">
        /// This method sets this value to indicate whether the enum is nullable. True indicates
        /// that it is nullable. False indicates that it is not nullable.
        /// </param>
        /// <returns>Name of the schema's C# type representation.</returns>
        internal static string getType(Schema schema, bool nullible, ref bool nullibleEnum)
        {
            switch (schema.Tag)
            {
                case Schema.Type.Null:
                    return typeof(object).ToString();
                case Schema.Type.Boolean:
                    if (nullible) return $"System.Nullable<{typeof(bool)}>";
                    else return typeof(bool).ToString();
                case Schema.Type.Int:
                    if (nullible) return $"System.Nullable<{typeof(int)}>";
                    else return typeof(int).ToString();
                case Schema.Type.Long:
                    if (nullible) return $"System.Nullable<{typeof(long)}>";
                    else return typeof(long).ToString();
                case Schema.Type.Float:
                    if (nullible) return $"System.Nullable<{typeof(float)}>";
                    else return typeof(float).ToString();
                case Schema.Type.Double:
                    if (nullible) return $"System.Nullable<{typeof(double)}>";
                    else return typeof(double).ToString();

                case Schema.Type.Bytes:
                    return typeof(byte[]).ToString();
                case Schema.Type.String:
                    return typeof(string).ToString();

                case Schema.Type.Enumeration:
                    var namedSchema = schema as NamedSchema;
                    if (null == namedSchema)
                        throw new CodeGenException("Unable to cast schema into a named schema");
                    if (nullible)
                    {
                        nullibleEnum = true;
                        return "System.Nullable<" + CodeGenUtil.Instance.Mangle(namedSchema.Fullname) + ">";
                    }
                    else return CodeGenUtil.Instance.Mangle(namedSchema.Fullname);

                case Schema.Type.Fixed:
                case Schema.Type.Record:
                case Schema.Type.Error:
                    namedSchema = schema as NamedSchema;
                    if (null == namedSchema)
                        throw new CodeGenException("Unable to cast schema into a named schema");
                    return CodeGenUtil.Instance.Mangle(namedSchema.Fullname);

                case Schema.Type.Array:
                    var arraySchema = schema as ArraySchema;
                    if (null == arraySchema)
                        throw new CodeGenException("Unable to cast schema into an array schema");

                    return "IList<" + getType(arraySchema.ItemSchema, false, ref nullibleEnum) + ">";

                case Schema.Type.Map:
                    var mapSchema = schema as MapSchema;
                    if (null == mapSchema)
                        throw new CodeGenException("Unable to cast schema into a map schema");
                    return "IDictionary<string," + getType(mapSchema.ValueSchema, false, ref nullibleEnum) + ">";

                case Schema.Type.Union:
                    var unionSchema = schema as UnionSchema;
                    if (null == unionSchema)
                        throw new CodeGenException("Unable to cast schema into a union schema");
                    Schema nullibleType = getNullableType(unionSchema);
                    if (null == nullibleType)
                        return CodeGenUtil.Object;
                    else
                        return getType(nullibleType, true, ref nullibleEnum);

                case Schema.Type.Logical:
                    var logicalSchema = schema as LogicalSchema;
                    if (null == logicalSchema)
                        throw new CodeGenException("Unable to cast schema into a logical schema");
                    var csharpType = logicalSchema.LogicalType.GetCSharpType(nullible);
                    if (csharpType.IsGenericType && csharpType.GetGenericTypeDefinition() == typeof(Nullable<>))
                    {
                        return $"System.Nullable<{csharpType.GetGenericArguments()[0]}>";
                    }
                    else
                    {
                        return csharpType.ToString();
                    }

            }
            throw new CodeGenException("Unable to generate CodeTypeReference for " + schema.Name + " type " + schema.Tag);
        }

        /// <summary>
        /// Gets the schema of a union with null
        /// </summary>
        /// <param name="schema">union schema</param>
        /// <returns>schema that is nullible</returns>
        public static Schema getNullableType(UnionSchema schema)
        {
            Schema ret = null;
            if (schema.Count == 2)
            {
                bool nullable = false;
                foreach (Schema childSchema in schema.Schemas)
                {
                    if (childSchema.Tag == Schema.Type.Null)
                        nullable = true;
                    else
                        ret = childSchema;
                }
                if (!nullable)
                    ret = null;
            }
            return ret;
        }

        /// <summary>
        /// Creates the static schema field for class types
        /// </summary>
        /// <param name="schema">schema</param>
        /// <param name="ctd">CodeTypeDeclaration for the class</param>
        /// <param name="overrideFlag">
        /// Indicates whether we should add the <see cref="MemberAttributes.Override"/> to the
        /// generated property.
        /// </param>
        protected virtual void createSchemaField(Schema schema, CodeTypeDeclaration ctd, bool overrideFlag)
        {
            // create schema field
            var ctrfield = new CodeTypeReference("Schema");
            string schemaFname = "_SCHEMA";
            var codeField = new CodeMemberField(ctrfield, schemaFname);
            codeField.Attributes = MemberAttributes.Public | MemberAttributes.Static;
            // create function call Schema.Parse(json)
            var cpe = new CodePrimitiveExpression(schema.ToString());
            var cmie = new CodeMethodInvokeExpression(
                new CodeMethodReferenceExpression(new CodeTypeReferenceExpression(typeof(Schema)), "Parse"),
                new CodeExpression[] { cpe });
            codeField.InitExpression = cmie;
            ctd.Members.Add(codeField);

            // create property to get static schema field
            var property = new CodeMemberProperty();
            property.Attributes = MemberAttributes.Public;
            if (overrideFlag) property.Attributes |= MemberAttributes.Override;
            property.Name = "Schema";
            property.Type = ctrfield;

            property.GetStatements.Add(new CodeMethodReturnStatement(new CodeTypeReferenceExpression(ctd.Name + "." + schemaFname)));
            ctd.Members.Add(property);
        }

        /// <summary>
        /// Creates an XML documentation for the given comment
        /// </summary>
        /// <param name="comment">comment</param>
        /// <returns>CodeCommentStatement object</returns>
        protected virtual CodeCommentStatement createDocComment(string comment)
        {
            string text = string.Format(CultureInfo.InvariantCulture,
                "<summary>{1} {0}{1} </summary>", comment, Environment.NewLine);
            return new CodeCommentStatement(text, true);
        }

        /// <summary>
        /// Writes the generated compile unit into one file
        /// </summary>
        /// <param name="outputFile">name of output file to write to</param>
        public virtual void WriteCompileUnit(string outputFile)
        {
            var cscp = new CSharpCodeProvider();

            var opts = new CodeGeneratorOptions();
            opts.BracingStyle = "C";
            opts.IndentString = "\t";
            opts.BlankLinesBetweenMembers = false;

            using (var outfile = new StreamWriter(outputFile))
            {
                cscp.GenerateCodeFromCompileUnit(CompileUnit, outfile, opts);
            }
        }

        /// <summary>
        /// Writes each types in each namespaces into individual files
        /// </summary>
        /// <param name="outputdir">name of directory to write to</param>
        public virtual void WriteTypes(string outputdir)
        {
            var cscp = new CSharpCodeProvider();

            var opts = new CodeGeneratorOptions();
            opts.BracingStyle = "C";
            opts.IndentString = "\t";
            opts.BlankLinesBetweenMembers = false;

            CodeNamespaceCollection nsc = CompileUnit.Namespaces;
            for (int i = 0; i < nsc.Count; i++)
            {
                var ns = nsc[i];

                string dir = outputdir;
                foreach (string name in CodeGenUtil.Instance.UnMangle(ns.Name).Split('.'))
                {
                    dir = Path.Combine(dir, name);
                }
                Directory.CreateDirectory(dir);

                var new_ns = new CodeNamespace(ns.Name);
                new_ns.Comments.Add(CodeGenUtil.Instance.FileComment);
                foreach (CodeNamespaceImport nci in CodeGenUtil.Instance.NamespaceImports)
                    new_ns.Imports.Add(nci);

                var types = ns.Types;
                for (int j = 0; j < types.Count; j++)
                {
                    var ctd = types[j];
                    string file = Path.Combine(dir, Path.ChangeExtension(CodeGenUtil.Instance.UnMangle(ctd.Name), "cs"));
                    using (var writer = new StreamWriter(file, false))
                    {
                        new_ns.Types.Add(ctd);
                        cscp.GenerateCodeFromNamespace(new_ns, writer, opts);
                        new_ns.Types.Remove(ctd);
                    }
                }
            }
        }
    }
}
