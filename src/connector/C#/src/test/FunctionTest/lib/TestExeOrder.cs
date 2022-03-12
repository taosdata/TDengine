using System;

namespace  Test.Case.Attributes
{
    [AttributeUsage(AttributeTargets.Method, AllowMultiple = false)]
    public class TestExeOrderAttribute : Attribute
    {
        public int ExeOrder { get; private set; }

        public TestExeOrderAttribute(int exeOrder) => ExeOrder = exeOrder;
    }
}