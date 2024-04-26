using System.Collections.Generic;
using System.Linq;
using System;
using Newtonsoft.Json;
using TDPIConnector.PI;
using TDPIConnector.Core.Conversions;

namespace TDPIConnector.Core
{
    using ScanPiInfo;
    namespace ScanPiInfo
    {
        public enum ScanMode
        {
            ScanNone,
            ScanPIInfo,
            ScanPoint,
            ScanPx,
            ScanPt
        };
        public enum FilterMode
        {
            FilterNone,
            FilterPoint,
            FilterElement,
            FilterTemplate
        };
        class ScanPointList
        {
            public List<ScanTemplateForPoint> Templates = new List<ScanTemplateForPoint>();
            public List<ScanPoint> Points = new List<ScanPoint>();
        }
        class ScanAFPointList
        {
            public List<ScanTemplateForAFPoint> Templates = new List<ScanTemplateForAFPoint>();
            public List<ScanAFPoint> Points = new List<ScanAFPoint>();
        }
        class ScanElementList
        {
            public List<ScanElementTemplate> Templates = new List<ScanElementTemplate>();
            public List<ScanSingleElement> SingleElements = new List<ScanSingleElement>();
            public List<ScanElement> Elements = new List<ScanElement>();
        }
        class ScanPointTags
        {
            public string PointID;
            public string PointClass;
            public string PointSource;
            public string EngUnits;
            public string Descriptor;
            public string Exdesc;
            public string SourceTag;
        }
        class ScanPoint
        {
            public int ID;
            public string Name;
            public string Path;
            public string Type;
            public string TDType;
            public string Template;
        }
        public class ScanAttributeValue
        {
            public string Name;
            public string Type;
            public string Value;
        }
        public class ScanAttribute
        {
            public string Name;
            public string Type;
            public string UOMABB;
            public string UOM;
        }
        class ScanAFPoint
        {
            public int ID;
            public string Name;
            public string Path;
            public string Type;
            public string TDType;
            public string UOMABB;
            public string UOM;
            public string Template;
            public Dictionary<string, string> Tags;
            public List<ScanElementSummary> Elements = new List<ScanElementSummary>();
        }
        class ScanElement
        {
            public string ID;
            public string Name;
            public string TemplateName;
            public string Path;
            public List<ScanAttributeValue> StaticAttributeValues;
        }
        class ScanElementSummary
        {
            public string ID;
            public string Name;
            public string TemplateName;
            public string Path;
        }
        class ScanTemplateForPoint
        {
            public string TemplateName;
            public string Type;
            public Dictionary<string, string> Tags;

            public string TDType { get; internal set; }
        }
        class ScanTemplateForAFPoint
        {
            public string TemplateName;
            public string TDType;
            public string Type;
            public string UOMABB;
            public string UOM;
            public Dictionary<string, string> Tags;
        }
        class ScanElementTemplate
        {
            public string TemplateName;
            public List<ScanAttribute> Attributes;
            public List<ScanAttribute> StaticAttributes;
        }
        class ScanSingleElement
        {
            public string ID;
            public string Name;
            public List<ScanAttribute> Attributes;
            public List<ScanAttribute> StaticAttributes;
        }

    }

    public class PIInfoScanner
    {
        private PIServerManager piServerManager;
        private PISystemManager piSystemManager;

        class PIInfo {
            public List<string> pointsName;
            public List<string> templateName;
            public List<string> elementNoTemplate;
        }

        public PIInfoScanner(PIServerManager piServerManager, PISystemManager piSystemManager)
        {
            this.piServerManager = piServerManager;
            this.piSystemManager = piSystemManager;
        }

        internal string GetInfo(ScanPiInfo.ScanMode scanMode, string filter, ScanPiInfo.FilterMode filterMode)
        {
            if (piServerManager == null)
            {
                return "PI System not found!";
            }
            switch (scanMode) {
                case ScanMode.ScanPIInfo:
                    return GetPIInfo(filter);
                case ScanMode.ScanPoint:
                    return GetScanPointInfo(filter);
                case ScanMode.ScanPx:
                    return GetScanAFPointInfo(filter, filterMode);
                case ScanMode.ScanPt:
                    return GetScanElementInfo(filter, filterMode);
                default:
                    return "start param error, scanMode not found!";
            }
        }

        internal string GetPIInfo(string filter)
        {
            var points = piServerManager.FindPIPoints(filter);

            var piInfo = new PIInfo();
            piInfo.pointsName = points.Select(p => p.Name).ToList();
            piInfo.templateName = new List<string>{ };
            if (piSystemManager != null) {
                var templates = piSystemManager.GetElementTemplates(AppSettings.tomlConfig.AFDatabaseName);
                piInfo.templateName = templates.Select(t => t.Name).ToList();
                var elements = piSystemManager.GetElementsNoTemplate(AppSettings.tomlConfig.AFDatabaseName);
                piInfo.elementNoTemplate = elements.Select(t => t.Name).ToList();
            }
            var json = JsonConvert.SerializeObject(piInfo);
            return json;
        }
        internal string GetScanPointInfo(string pointFilter)
        {
            var points = piServerManager.FindPIPoints(pointFilter);
            var piInfo = new ScanPointList();

            HashSet<string> existTemplate = new HashSet<string>();

            foreach (var point in points) {
                var tName =  GeneratePointSuperTableName(point);
                if (!existTemplate.Contains(tName)) {
                    existTemplate.Add(tName);
                    ScanTemplateForPoint t = new ScanTemplateForPoint();
                    t.TemplateName = tName;
                    t.TDType = PointTypeConverter.Convert(point.PointType);
                    t.Type = point.PointType;
                    t.Tags = PIPointWrapper.GetPointSavedAttrsType();
                    piInfo.Templates.Add(t);
                }
                ScanPoint p = new ScanPoint();
                p.Path = point.GetPath();
                p.ID = point.ID;
                p.Name = point.Name;
                p.Type = point.PointType;
                p.TDType = PointTypeConverter.Convert(point.PointType);
                p.Template = tName;
                piInfo.Points.Add(p);
            }

            var json = JsonConvert.SerializeObject(piInfo);
            return json;
        }
        internal string GetScanAFPointInfo(string filter, FilterMode filterMode)
        {
            if (piSystemManager == null)
            {
                return "PI System not found!";
            }
            if (FilterMode.FilterElement == filterMode) {
                return GetScanAFPointInfoByElementFilter(ref filter);
            } else if (FilterMode.FilterTemplate == filterMode) {
                return GetScanAFPointInfoByTemplateFilter(ref filter);
            } else {
                return "start param error, filterMode not found!";
            }
        }

        public string GetAFPointTemplateName(AFAttributeWrapper attr)
        {
            if (attr.Uom != null) { 
                return "TS_" + attr.Type.Name + "_" + attr.Uom; 
            } else
            {
                return "TS_" + attr.Type.Name;
            }
        }
        internal string GetScanAFPointInfoByElements(IEnumerable<AFElementWrapper> elements)
        {
            var piInfo = new ScanAFPointList();
            HashSet<string> existTemplate = new HashSet<string>();
            HashSet<Guid> existElements = new HashSet<Guid>();
            Dictionary<int, ScanAFPoint> points = new Dictionary<int, ScanAFPoint>();

            foreach (var element in elements)
            {
                if (!existElements.Contains(element.ID))
                {
                    existElements.Add(element.ID);
                    foreach (var attr in element.Attributes)
                    {
                        if (attr.IsTDengineTag() || attr.Unsupported()) continue;
                        var templateName = GetAFPointTemplateName(attr);
                        if (!existTemplate.Contains(templateName))
                        {
                            existTemplate.Add(templateName);
                            ScanTemplateForAFPoint temp = new ScanTemplateForAFPoint();
                            temp.TemplateName = templateName;
                            temp.TDType = AttributeTypeConverter.Convert(attr.DataReference, attr.Type);
                            temp.Type = attr.Type.Name;
                            temp.UOMABB = attr.Uom;
                            temp.UOM = attr.UomName;
                            temp.Tags = PIPointWrapper.GetPointSavedAttrsType();
                            piInfo.Templates.Add(temp);
                        }
                        ScanElementSummary e = new ScanElementSummary();
                        e.ID = element.ID.ToString();
                        e.Name = element.Name;
                        e.Path = element.GetPath();
                        e.TemplateName = element.hasTemplate() ? element.Template.Name: "";

                        if (attr.PIPoint != null)
                        {
                            if (!points.ContainsKey(attr.PIPoint.PointId))
                            {
                                ScanAFPoint point = new ScanAFPoint();
                                point.ID = attr.PIPoint.PointId;
                                point.Name = attr.PIPoint.Name;
                                point.Type = attr.Type.Name;
                                point.TDType = AttributeTypeConverter.Convert(attr.DataReference, attr.Type);
                                point.UOMABB = attr.Uom;
                                point.UOM = attr.UomName;
                                point.Template = templateName;
                                point.Path = attr.PIPoint.GetPath();
                                point.Tags = attr.PIPoint.GetPointSavedAttrsValue();
                                point.Elements.Add(e);
                                points.Add(attr.PIPoint.PointId, point);
                            }
                            else
                            {
                                points[attr.PIPoint.PointId].Elements.Add(e);
                            }
                        }
                    }
                }
            }
            foreach (var p in points)
            {
                piInfo.Points.Add(p.Value);
            }

            var json = JsonConvert.SerializeObject(piInfo);
            return json;
        }
        internal string GetScanAFPointInfoByElementFilter(ref string filter)
        {
            var elements = piSystemManager.GetElementByFilter(AppSettings.tomlConfig.AFDatabaseName, filter);
            return GetScanAFPointInfoByElements(elements);
        }
        internal string GetScanAFPointInfoByTemplateFilter(ref string filter)
        {
            IEnumerable<AFElementWrapper> elements = new List<AFElementWrapper>();
            var templates = piSystemManager.GetElementTemplates(AppSettings.tomlConfig.AFDatabaseName, filter);
            foreach (var template in templates)
            {
                var es = piSystemManager.GetElementsByTemplate(AppSettings.tomlConfig.AFDatabaseName, template.Name);
                elements = elements.Concat(es);
            }

            return GetScanAFPointInfoByElements(elements);
        }
        internal string GetScanElementInfo(string filter, FilterMode filterMode)
        {
            if (piSystemManager == null)
            {
                return "PI System not found!";
            }
            if (FilterMode.FilterElement == filterMode)
            {
                return GetScanElementInfoByElementFilter(ref filter);
            }
            else if (FilterMode.FilterTemplate == filterMode)
            {
                return GetScanElementInfoByTemplateFilter(ref filter);
            }
            else
            {
                return "start param error, filterMode not found!";
            }
        }
        internal string GetScanElementInfoByElements(IEnumerable<AFElementWrapper> elements) {

            ScanElementList elmentInfo = new ScanElementList();
            HashSet<string> existTemplates = new HashSet<string>();          
            HashSet<Guid> usedElements = new HashSet<Guid>();
            foreach (var element in elements) {
                if (!usedElements.Contains(element.ID)) {
                    usedElements.Add(element.ID);

                    if (element.hasTemplate())
                    {
                        if (!existTemplates.Contains(element.Template.Name)) {
                            existTemplates.Add(element.Template.Name);
                            ScanElementTemplate template = new ScanElementTemplate();
                            template.TemplateName = element.Template.Name;
                            template.Attributes = GetTemplateAtrributes(element.Template);
                            template.StaticAttributes = GetTemplateAtrributesForTag(element.Template);
                            elmentInfo.Templates.Add(template);
                        }
                    }
                    else
                    {
                        ScanSingleElement temp = new ScanSingleElement();
                        temp.ID = element.ID.ToString();
                        temp.Name = element.Name;
                        temp.Attributes = GetElementAtrributes(element);
                        if (temp.Attributes.Count == 0) continue;
                        temp.StaticAttributes = GetElementAtrributesForTag(element);
                        elmentInfo.SingleElements.Add(temp);
                    }
                    ScanElement e = new ScanElement();
                    e.ID = element.ID.ToString();
                    e.Name = element.Name;
                    e.Path = element.GetPath();
                    e.TemplateName = element.hasTemplate() ? element.Template.Name: "";
                    e.StaticAttributeValues = GetElementStaticAtrributeValues(element);
                    elmentInfo.Elements.Add(e);
                }
            }
            var json = JsonConvert.SerializeObject(elmentInfo);
            return json;
        }
        internal string GetScanElementInfoByElementFilter(ref string filter) {
            var elements = piSystemManager.GetElementByFilter(AppSettings.tomlConfig.AFDatabaseName, filter);
            return GetScanElementInfoByElements(elements);
        }
        internal string GetScanElementInfoByTemplateFilter(ref string filter)
        {
            IEnumerable<AFElementWrapper> elements = new List<AFElementWrapper>();
            var templates = piSystemManager.GetElementTemplates(AppSettings.tomlConfig.AFDatabaseName, filter);
            foreach (var template in templates)
            {
                var es = piSystemManager.GetElementsByTemplate(AppSettings.tomlConfig.AFDatabaseName, template.Name);
                elements = elements.Concat(es);
            }
            var elementsWithoutTemplate = piSystemManager.GetElementsNoTemplate(AppSettings.tomlConfig.AFDatabaseName);
            elements = elements.Concat(elementsWithoutTemplate);

            return GetScanElementInfoByElements(elements);
        }
        static public FilterMode GetFilterMode(string strMode)
        {
            if ("element" == strMode || "e" == strMode ||
                "Element" == strMode || "ELEMENT" == strMode ) return FilterMode.FilterElement;
            else return FilterMode.FilterTemplate;
        }

        static public List<ScanAttribute> GetTemplateAtrributes(AFElementTemplateWrapper template) {
            List<ScanAttribute> attributes = new List<ScanAttribute>();
            foreach (var attr in template.AttributeTemplates) {
                if (!attr.IsTDengineTag())
                {
                    ScanAttribute tmp = new ScanAttribute();
                    tmp.Type = AttributeTypeConverter.Convert(attr.DataReference, attr.Type);
                    if (null == tmp.Type) continue;
                    tmp.Name = attr.Name;
                    tmp.UOMABB = attr.Uom;
                    tmp.UOM = attr.UomName;
                    attributes.Add(tmp);
                }
            }
            return attributes;
        }
        static public List<ScanAttribute> GetTemplateAtrributesForTag(AFElementTemplateWrapper template)
        {
            List<ScanAttribute> attributes = new List<ScanAttribute>();
            foreach (var attr in template.AttributeTemplates)
            {
                if (attr.IsTDengineTag()) {
                    ScanAttribute tmp = new ScanAttribute();
                    tmp.Type = AttributeTypeConverter.Convert(attr.DataReference, attr.Type);
                    if (null == tmp.Type) continue;
                    tmp.Name = attr.Name;
                    tmp.UOMABB = attr.Uom;
                    tmp.UOM = attr.UomName;
                    attributes.Add(tmp);
                }
            }
            return attributes;
        }
        static public List<ScanAttribute> GetElementAtrributes(AFElementWrapper element)
        {
            List<ScanAttribute> attributes = new List<ScanAttribute>();
            foreach (var attr in element.Attributes)
            {
                if (!attr.IsTDengineTag())
                {
                    ScanAttribute tmp = new ScanAttribute();
                    tmp.Type = AttributeTypeConverter.Convert(attr.DataReference, attr.Type);
                    if (null == tmp.Type) continue;
                    tmp.Name = attr.Name;
                    tmp.UOMABB = attr.Uom;
                    attributes.Add(tmp);
                }
            }
            return attributes;
        }
        static public List<ScanAttribute> GetElementAtrributesForTag(AFElementWrapper element)
        {
            List<ScanAttribute> attributes = new List<ScanAttribute>();
            foreach (var attr in element.Attributes)
            {
                if (attr.IsTDengineTag())
                {
                    ScanAttribute tmp = new ScanAttribute();
                    tmp.Type = AttributeTypeConverter.Convert(attr.DataReference, attr.Type);
                    if (null == tmp.Type) continue;
                    tmp.Name = attr.Name;
                    tmp.UOMABB = attr.Uom;
                    attributes.Add(tmp);
                }
            }
            return attributes;
        }
        static public List<ScanAttributeValue> GetElementStaticAtrributeValues(AFElementWrapper element)
        {
            List<ScanAttributeValue> attributeValues = new List<ScanAttributeValue>();
            foreach (var attr in element.Attributes)
            {
                if (attr.IsTDengineTag())
                {
                    ScanAttributeValue v = new ScanAttributeValue();
                    v.Name = attr.Name;
                    v.Type = AttributeTypeConverter.Convert(attr.DataReference, attr.Type);
                    v.Value = attr.GetValueString();
                    attributeValues.Add(v);
                }
            }
            return attributeValues;
        }
        static public string GeneratePointSuperTableName(PIPointWrapper point)
        {    
            return "ts_" + point.PointType.ToLower();
        }
    }
}
