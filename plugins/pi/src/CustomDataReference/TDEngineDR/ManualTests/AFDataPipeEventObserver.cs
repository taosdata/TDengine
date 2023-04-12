using OSIsoft.AF.Asset;
using OSIsoft.AF.Data;
using System;

namespace ManualTests
{
    internal class AFDataPipeEventObserver : IObserver<AFDataPipeEvent>
    {
        public void OnCompleted()
        {

        }

        public void OnError(Exception error)
        {

        }

        public void OnNext(AFDataPipeEvent value)
        {
            AFAttribute attr = value.Value.Attribute;
            Console.WriteLine(DateTime.Now.ToString() + " " + attr.Element.GetPath() + " " + attr.Name + "  " + value.Value.Value);
        }
    }
}