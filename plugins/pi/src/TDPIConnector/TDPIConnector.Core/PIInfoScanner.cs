using System.Collections.Generic;
using System.Linq;
using System;
using Newtonsoft.Json;
using TDPIConnector.PI;

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
            public List<ScanTemplateForPoint> Template;
            public List<ScanPoint> PointList;
        }
        class ScanAFPointList
        {
            public List<ScanTemplateForPoint> Templates = new List<ScanTemplateForPoint>();
            public List<ScanAFPoint> Points = new List<ScanAFPoint>();
        }
        class ScanElementList
        {
            public List<ScanElementTemplate> Templates;
            public List<ScanSingleElement> SingleElements;
            public List<ScanElement> elements;
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
            public string Path;
            public string Template;
        }
        class ScanAttribute
        {
            public string type;
            public string UOM;
            public bool   IsTag;
        }
        class ScanAFPoint
        {
            public int ID;
            public string Path;
            public string Type;
            public string UOM;
            public string Template;
            public Dictionary<string, string> Tags;
            public List<ScanElement> Elements = new List<ScanElement>();
        }
        class ScanElement
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
            public string UOM;
            public Dictionary<string, string> Tags;
        }
        class ScanElementTemplate
        {
            public string TemplateName;
            public ScanAttribute attributes;
        }
        class ScanSingleElement
        {
            public ScanAttribute attributes;
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
            if (piSystemManager == null)
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

            foreach (var point in points) {
                ScanTemplateForPoint t = new ScanTemplateForPoint();
                point.GetPointSavedAttrsValue();

            }

            var json = JsonConvert.SerializeObject(piInfo);
            return json;
        }
        internal string GetScanAFPointInfo(string filter, FilterMode filterMode)
        {
            if (FilterMode.FilterElement == filterMode) {
                return GetScanAFPointInfoByElementFilter(ref filter);
            } else if (FilterMode.FilterTemplate == filterMode) {
                return GetScanAFPointInfoByTemplateFilter(filter);
            } else {
                return "start param error, filterMode not found!";
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
                        var templateName = element.GetAFPointTemplateName(attr);
                        if (!existTemplate.Contains(templateName))
                        {
                            existTemplate.Add(templateName);
                            ScanTemplateForPoint temp = new ScanTemplateForPoint();
                            temp.TemplateName = templateName;
                            temp.Type = attr.Type.Name;
                            temp.UOM = attr.Uom;
                            temp.Tags = PIPointWrapper.GetPointSavedAttrsType();
                            piInfo.Templates.Add(temp);
                        }
                        ScanElement e = new ScanElement();
                        e.ID = element.ID.ToString();
                        e.Name = element.Name;
                        e.Path = element.GetPath();
                        e.TemplateName = templateName;

                        if (attr.PIPoint != null)
                        {
                            if (!points.ContainsKey(attr.PIPoint.PointId))
                            {
                                ScanAFPoint point = new ScanAFPoint();
                                point.ID = attr.PIPoint.PointId;
                                point.Type = attr.Type.Name;
                                point.UOM = attr.Uom;
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
        internal string GetScanAFPointInfoByTemplateFilter(string filter)
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
        internal string GetScanElementInfo(string pointFilter, FilterMode filterMode)
        {
            var points = piServerManager.FindPIPoints(pointFilter);

            var piInfo = new ScanElementList();
            var json = JsonConvert.SerializeObject(piInfo);
            return json;
        }

        static public FilterMode GetFilterMode(string strMode)
        {
            if ("element" == strMode || "e" == strMode ||
                "Element" == strMode || "ELEMENT" == strMode ) return FilterMode.FilterElement;
            else return FilterMode.FilterTemplate;
        }
    }
}
