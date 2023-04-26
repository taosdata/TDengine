using OSIsoft.AF.Asset;
using System;

namespace PISystemWrapper
{
    public class AFEnumerationValueWrapper
    {
        private AFEnumerationValue enumValue;

        public AFEnumerationValueWrapper(AFEnumerationValue enumValue)
        {
            this.enumValue = enumValue;
        }

        public int Value
        {
            get
            {
                return this.enumValue.Value;
            }
        }

        public string GetEnumerationSetName()
        {
            return this.enumValue.EnumerationSet.Name;
        }

        public string Name
        {
            get
            {
                return this.enumValue.Name;
            }
        }

    }
}