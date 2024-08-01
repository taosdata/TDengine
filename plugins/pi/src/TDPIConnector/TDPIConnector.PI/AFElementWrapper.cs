using OSIsoft.AF.Asset;
using System;

namespace TDPIConnector.PI
{
    public class ElementWrapperPool
    {
       //  private readonly ObjectPool<AFElementWrapper> _pool;
       // 
       //  public ElementWrapperPool()
       //  {
       //      _pool = new DefaultObjectPool<AFElementWrapper>(new DefaultPooledObjectPolicy<AFElementWrapper>());
       //  }
       // 
       //  public AFElementWrapper Rent()
       //  {
       //      return _pool.Get();
       //  }
       // 
       //  public void Return(AFElementWrapper wrapper)
       //  {
       //      _pool.Return(wrapper);
       //  }
    }
    public class AFElementWrapper
    {
        internal AFElement AFSDKObject { get; private set; }
        internal AFElementWrapper(in AFElement element)
        {
            this.AFSDKObject = element;
        }
        public AFElementWrapper()
        {

        }

        public bool isNull()
        {
            return AFSDKObject == null;
        }

        public AFElementTemplateWrapper Template
        {
            get
            {
                return new AFElementTemplateWrapper(this.AFSDKObject.Template);
            }
        }
        public virtual string Name
        {
            get
            {
                return this.AFSDKObject.Name;
            }
        }
        public virtual Guid ID
        {
            get
            {
                return this.AFSDKObject.ID;
            }
        }
        public AFAttributesWrapper Attributes
        {
            get
            {
                return new AFAttributesWrapper(this.AFSDKObject.Attributes);
            }
        }

        public string CategoriesString
        {
            get
            {
                return this.AFSDKObject.CategoriesString;
            }
        }

        public AFDatabaseWrapper Database
        {
            get
            {
                return new AFDatabaseWrapper(this.AFSDKObject.Database);
            }
        }
        public virtual string GetPath()
        {
            return this.AFSDKObject.GetPath();
        }
        public bool HasInvalidAttr()
        {
            foreach (var att in this.AFSDKObject.Attributes)
            {
                try
                {
                    if (att.PIPoint == null)
                        ;// do noting, just for exception
                }
                catch (Exception e)
                {
                      return true;
                }
            }
            return false;
        }
        public bool hasTemplate() {
            return Template.AFSDKObject != null;
        }

        public string TemplateName() {
            if (hasTemplate())
            {
                return Template.AFSDKObject.Name;
            }
            else {
                return this.AFSDKObject.Name;
            }
        }
    }
}
