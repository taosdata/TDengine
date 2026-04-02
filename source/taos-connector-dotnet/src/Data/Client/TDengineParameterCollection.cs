using System;
using System.Collections;
using System.Collections.Generic;
using System.Data.Common;

namespace TDengine.Data.Client
{
    public class TDengineParameterCollection : DbParameterCollection
    {
        private readonly List<TDengineParameter> _parameters = new List<TDengineParameter>();

        public override int Add(object value)
        {
            _parameters.Add((TDengineParameter)value);
            return _parameters.Count - 1;
        }

        public override void Clear()
        {
            _parameters.Clear();
        }

        public override bool Contains(object value)
        {
            return _parameters.Contains((TDengineParameter)value);
        }

        public override int IndexOf(object value)
        {
            return _parameters.IndexOf((TDengineParameter)value);
        }

        public override void Insert(int index, object value)
        {
            _parameters.Insert(index, (TDengineParameter)value);
        }

        public override void Remove(object value)
        {
            _parameters.Remove((TDengineParameter)value);
        }

        public override void RemoveAt(int index)
        {
            _parameters.RemoveAt(index);
        }

        public override void RemoveAt(string parameterName)
        {
            RemoveAt(GetIndex(parameterName));
        }

        protected override void SetParameter(int index, DbParameter value)
        {
            this[index] = (TDengineParameter)value;
        }

        protected override void SetParameter(string parameterName, DbParameter value)
        {
            SetParameter(GetIndex(parameterName), value);
        }

        public override int Count => _parameters.Count;
        public override bool IsFixedSize { get; }
        public override bool IsReadOnly { get; }
        public override bool IsSynchronized { get; }
        public override object SyncRoot => ((ICollection)_parameters).SyncRoot;

        public override int IndexOf(string parameterName)
        {
            for (var index = 0; index < _parameters.Count; index++)
            {
                if (_parameters[index].ParameterName == parameterName)
                {
                    return index;
                }
            }

            return -1;
        }

        public override bool Contains(string value)
        {
            return IndexOf(value) != -1;
        }

        public override void CopyTo(Array array, int index)
        {
            _parameters.CopyTo((TDengineParameter[])array, index);
        }

        public override IEnumerator GetEnumerator()
        {
            return _parameters.GetEnumerator();
        }

        protected override DbParameter GetParameter(int index)
        {
            return this[index];
        }

        protected override DbParameter GetParameter(string parameterName)
        {
            var index = GetIndex(parameterName);
            return this[index];
        }

        public override void AddRange(Array values)
        {
            List<TDengineParameter> result = new List<TDengineParameter>();

            foreach (var value in values)
            {
                if (value is TDengineParameter parameter)
                {
                    result.Add(parameter);
                }
            }
            _parameters.AddRange(result);
        }

        public new virtual TDengineParameter this[int index]
        {
            get => _parameters[index];
            set => _parameters[index] = value;
        }

        private int GetIndex(string parameterName)
        {
            var index = IndexOf(parameterName);
            if (index == -1)
            {
                throw new IndexOutOfRangeException($"ParameterNotFound{parameterName}");
            }

            return index;
        }

        private int _param_index = 0;

        public virtual TDengineParameter AddWithValue(object value)
        {
            _param_index++;
            var parameter = new TDengineParameter($"@{_param_index:0000}", value);
            Add(parameter);
            return parameter;
        }

        public virtual TDengineParameter AddWithTag(object value)
        {
            _param_index++;
            var parameter = new TDengineParameter($"${_param_index:0000}", value);
            Add(parameter);
            return parameter;
        }
        
        public virtual TDengineParameter AddWithTableName(object value)
        {
            _param_index++;
            var parameter = new TDengineParameter($"#{_param_index:0000}", value);
            Add(parameter);
            return parameter;
        }

        public virtual TDengineParameter AddWithValue(string parameterName, object value)
        {
            var parameter = new TDengineParameter(parameterName, value);
            Add(parameter);
            return parameter;
        }
    }
}