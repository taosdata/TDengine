using System;
using System.Data;
using System.Data.Common;

namespace TDengine.Data.Client
{
    public class TDengineParameter : DbParameter
    {
        private string _parameterName = string.Empty;
        private object _value;
        private int? _size;

        public override void ResetDbType()
        {
            DbType = DbType.String;
        }

        public TDengineParameter()
        {
            
        }
        public TDengineParameter(string name, object value)
        {
            if (string.IsNullOrEmpty(name))
            {
                throw new ArgumentNullException(nameof(name));
            }

            _parameterName = name;
            _value = value;
        }
        
        public override DbType DbType { get; set; } = DbType.Single;

        public override ParameterDirection Direction
        {
            get => ParameterDirection.Input;
            set
            {
                if (value != ParameterDirection.Input)
                {
                    throw new ArgumentException($"InvalidParameterDirection{value}");
                }
            }
        }

        public override bool IsNullable { get; set; }

        public override string ParameterName
        {
            get => _parameterName;
            set
            {
                if (string.IsNullOrEmpty(value))
                {
                    throw new ArgumentNullException(nameof(Value));
                }

                _parameterName = value;
            }
        }

        public override string SourceColumn { get; set; } = string.Empty;
        public override DataRowVersion SourceVersion { get; set; }

        public override object Value
        {
            get => _value;
            set => _value = value;
        }

        public override bool SourceColumnNullMapping { get; set; }

        /// <summary>Gets or sets the maximum size, in bytes, of the data within the column.</summary>
        /// <returns>The maximum size, in bytes, of the data within the column. The default value is inferred from the parameter value.</returns>
        public override int Size
        {
            get
            {
                if (_size != null)
                {
                    return _size.Value;
                }

                switch (_value)
                {
                    case string stringValue:
                        return stringValue.Length;
                    case byte[] bytes:
                        return bytes.Length;
                    default:
                        return 0;
                }
            }
            set
            {
                if (value < 0)
                {
                    throw new ArgumentOutOfRangeException(nameof(value), value, message: "unavailable parameter size");
                }

                _size = value;
            }
        }
    }
}