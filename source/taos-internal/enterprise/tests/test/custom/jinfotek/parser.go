package main

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

const (
	OUTPUT_FIELD_INT64  = 0
	OUTPUT_FIELD_DOUBLE = 1
	OUTPUT_FIELD_STRING = 2
)

var g_outputFields = []struct {
	name string
	typ  uint8
}{
	{"SEQNUM", OUTPUT_FIELD_INT64},
	{"IMSI", OUTPUT_FIELD_INT64},
	{"IMEI", OUTPUT_FIELD_STRING},
	{"LAC", OUTPUT_FIELD_INT64},
	{"EQUID", OUTPUT_FIELD_STRING},
	{"LACINCTIMER", OUTPUT_FIELD_INT64},
	{"LACSETTIME", OUTPUT_FIELD_STRING},
	{"HOMEAREA", OUTPUT_FIELD_STRING},
	{"MSISDN", OUTPUT_FIELD_STRING},
	{"SPCODE", OUTPUT_FIELD_INT64},
	{"IMPTIME", OUTPUT_FIELD_STRING},
	{"SYSTEM", OUTPUT_FIELD_INT64},
	{"LONGITUDE", OUTPUT_FIELD_DOUBLE},
	{"LATITUDE", OUTPUT_FIELD_DOUBLE},
	{"PN", OUTPUT_FIELD_INT64},
	{"FREQ", OUTPUT_FIELD_INT64},
	{"MAC", OUTPUT_FIELD_STRING},
	{"SMSSENDSTATUS", OUTPUT_FIELD_INT64},
	{"RSSI", OUTPUT_FIELD_INT64},
	{"ESN", OUTPUT_FIELD_STRING},
	{"TMSI", OUTPUT_FIELD_STRING},
	{"AREACODE", OUTPUT_FIELD_STRING},
	{"RECORDTYPE", OUTPUT_FIELD_STRING},
	{"RELATENUM", OUTPUT_FIELD_STRING},
	{"RELATEHOMEAC", OUTPUT_FIELD_STRING},
	{"CURAREA", OUTPUT_FIELD_STRING},
	{"NEID", OUTPUT_FIELD_STRING},
	{"LAI", OUTPUT_FIELD_STRING},
	{"CI", OUTPUT_FIELD_STRING},
	{"BILLTYPE", OUTPUT_FIELD_STRING},
	{"CALLTYPE", OUTPUT_FIELD_STRING},
	{"DTMF", OUTPUT_FIELD_STRING},
	{"CALLDURATION", OUTPUT_FIELD_INT64},
	{"CAUSE", OUTPUT_FIELD_INT64},
	{"RLGTIME", OUTPUT_FIELD_INT64},
	{"ALERTTIME", OUTPUT_FIELD_INT64},
	{"CONNECTTIME", OUTPUT_FIELD_INT64},
	{"DISCONNECTTIME", OUTPUT_FIELD_INT64},
	{"SID", OUTPUT_FIELD_STRING},
	{"IDFLAG", OUTPUT_FIELD_INT64},
	{"RAWRELATENUM", OUTPUT_FIELD_STRING},
	{"REDIRFLAG", OUTPUT_FIELD_INT64},
	{"ORIGCALLEDNO", OUTPUT_FIELD_STRING},
	{"DISCONNECTTYPE", OUTPUT_FIELD_INT64},
	{"NEWLAI", OUTPUT_FIELD_STRING},
	{"NEWCI", OUTPUT_FIELD_STRING},
	{"NEWLONGITUDE", OUTPUT_FIELD_DOUBLE},
	{"NEWLATITUDE", OUTPUT_FIELD_DOUBLE},
	{"VOICEFLAG", OUTPUT_FIELD_STRING},
	{"VOICEKEYA", OUTPUT_FIELD_STRING},
	{"VOICEKEYB", OUTPUT_FIELD_STRING},
	{"PEERSID", OUTPUT_FIELD_STRING},
	{"OLDLAI", OUTPUT_FIELD_STRING},
	{"OLDCI", OUTPUT_FIELD_STRING},
	{"OLDLONGITUDE", OUTPUT_FIELD_DOUBLE},
	{"OLDLATITUDE", OUTPUT_FIELD_DOUBLE},
	{"STATED", OUTPUT_FIELD_INT64},
	{"SENDTIME", OUTPUT_FIELD_INT64},
	{"MESSAGE", OUTPUT_FIELD_STRING},
	{"MSGTAG", OUTPUT_FIELD_STRING},
	{"BRAND", OUTPUT_FIELD_INT64},
	{"PCI", OUTPUT_FIELD_INT64},
	{"USERNAME", OUTPUT_FIELD_INT64},
	{"TERMINATECAUSE", OUTPUT_FIELD_INT64},
	{"WX_OPEN_ID", OUTPUT_FIELD_INT64},
	{"WX_TID", OUTPUT_FIELD_INT64},
	{"MSISDN_FY_TF", OUTPUT_FIELD_INT64},
	{"MSISDN_FY_TIME", OUTPUT_FIELD_INT64},
	{"IMSI_FY_TF", OUTPUT_FIELD_INT64},
	{"IMSI_FY_TIME", OUTPUT_FIELD_INT64},
	{"IMEI_FY_TF", OUTPUT_FIELD_INT64},
	{"IMEI_FY_TIME", OUTPUT_FIELD_INT64},
	{"MAC_FY_TF", OUTPUT_FIELD_INT64},
	{"MAC_FY_TIME", OUTPUT_FIELD_INT64},
	{"USERNAME_FY_TF", OUTPUT_FIELD_INT64},
	{"USERNAME_FY_TIME", OUTPUT_FIELD_INT64},
}

var g_rdFields = map[string]uint8{
	"HOMEAREA":         1,
	"MSISDN":           2,
	"IMSI":             3,
	"IMEI":             4,
	"TMSI":             5,
	"AREACODE":         6,
	"EQUID":            7,
	"LONGITUDE":        8,
	"LATITUDE":         9,
	"MAC":              10,
	"RSSI":             11,
	"LAC":              12,
	"INCTIMER":         13,
	"AREACODETIME":     14,
	"SPCODE":           15,
	"NETWORK":          16,
	"ESN":              17,
	"PN":               18,
	"FREQUENCE":        19,
	"SMSSENDSTATUS":    20,
	"SEQNUM":           21,
	"BRAND":            22,
	"PCI":              23,
	"USERNAME":         24,
	"TERMINATECAUSE":   25,
	"WX_OPEN_ID":       26,
	"WX_TID":           27,
	"MSISDN_FY_TF":     28,
	"MSISDN_FY_TIME":   29,
	"IMSI_FY_TF":       30,
	"IMSI_FY_TIME":     31,
	"IMEI_FY_TF":       32,
	"IMEI_FY_TIME":     33,
	"MAC_FY_TF":        34,
	"MAC_FY_TIME":      35,
	"USERNAME_FY_TF":   36,
	"USERNAME_FY_TIME": 37,
}

func rdParseRecord(str string) (*Record, error) {
	fields := strings.Split(str, ",")
	if len(fields) != 38 {
		return nil, fmt.Errorf("wrong column count, expect 38, actual %d", len(fields))
	}

	r := &Record{}

	// CATCHTIME
	if v, e := strconv.ParseInt(fields[0], 10, 64); e != nil {
		return nil, fmt.Errorf("invalid CATCHTIME(1): %s", fields[0])
	} else {
		r.catchtime = time.Date(
			int(v/10000000000),
			time.Month(v%10000000000/100000000),
			int(v%10000000/1000000),
			int(v%1000000/10000),
			int(v%10000/100),
			int(v%100),
			0,
			time.Local,
		).Unix()
	}

	sb := strings.Builder{}
	sb.WriteString(",0") // TIMESTAMP

	for _, f := range g_outputFields {
		sb.WriteByte(',')

		idx, ok := g_rdFields[f.name]
		if !ok {
			if f.typ == OUTPUT_FIELD_STRING {
				sb.WriteString("''")
			} else {
				sb.WriteString("null")
			}
			continue
		}

		s := fields[idx]
		if f.typ == OUTPUT_FIELD_STRING {
			sb.WriteByte('\'')
			sb.WriteString(fields[idx])
			sb.WriteByte('\'')
			continue
		}

		if len(s) == 0 {
			sb.WriteString("null")
			continue
		}

		var e error
		if f.typ == OUTPUT_FIELD_INT64 {
			_, e = strconv.ParseInt(s, 10, 64)
		} else if f.typ == OUTPUT_FIELD_DOUBLE {
			_, e = strconv.ParseFloat(s, 64)
		}

		if e != nil {
			return nil, fmt.Errorf("invalid %s(%d): %s", f.name, idx, s)
		} else {
			sb.WriteString(s)
		}
	}

	sb.WriteByte(')')
	r.values = sb.String()
	r.Type = 1
	r.equid = fields[g_rdFields["EQUID"]]
	r.tbname = "rd" + r.equid

	return r, nil
}

func parseRecord(fields []string, idxMap map[string]uint8) (*Record, error) {
	r := &Record{}

	// BEGINTIME
	if v, e := strconv.ParseInt(fields[0], 10, 64); e != nil {
		return nil, fmt.Errorf("invalid BEGINTIME(1): %s", fields[0])
	} else {
		r.catchtime = v
	}

	sb := strings.Builder{}
	sb.WriteString(",0") // TIMESTAMP

	for _, f := range g_outputFields {
		sb.WriteByte(',')

		if f.name == "EQUID" {
			spcode := fields[idxMap["SPCODE"]]
			lai := fields[idxMap["LAI"]]
			ci := fields[idxMap["CI"]]
			r.equid = fmt.Sprintf("%s-%s-%s", spcode, lai, ci)
			r.tbname = fmt.Sprintf("wfw%s_%s_%s", spcode, lai, ci)
			sb.WriteByte('\'')
			sb.WriteString(r.equid)
			sb.WriteByte('\'')
			continue
		}

		idx, ok := idxMap[f.name]
		if !ok {
			if f.typ == OUTPUT_FIELD_STRING {
				sb.WriteString("''")
			} else {
				sb.WriteString("null")
			}
			continue
		}

		s := fields[idx]
		if f.typ == OUTPUT_FIELD_STRING {
			sb.WriteByte('\'')
			sb.WriteString(fields[idx])
			sb.WriteByte('\'')
			continue
		}

		if len(s) == 0 {
			sb.WriteString("null")
			continue
		}

		var e error
		if f.typ == OUTPUT_FIELD_INT64 {
			_, e = strconv.ParseInt(s, 10, 64)
		} else if f.typ == OUTPUT_FIELD_DOUBLE {
			_, e = strconv.ParseFloat(s, 64)
		}

		if e != nil {
			return nil, fmt.Errorf("invalid %s(%d): %s", f.name, idx, s)
		} else {
			sb.WriteString(s)
		}
	}

	sb.WriteByte(')')
	r.values = sb.String()
	r.Type = 2

	return r, nil
}

var g_cdrFields = map[string]uint8{
	"MSISDN":         1,
	"HOMEAREA":       2,
	"RELATENUM":      3,
	"RELATEHOMEAC":   4,
	"IMSI":           5,
	"IMEI":           6,
	"CURAREA":        7,
	"NEID":           8,
	"LAI":            9,
	"CI":             10,
	"LONGITUDE":      11,
	"LATITUDE":       12,
	"BILLTYPE":       13,
	"CALLTYPE":       14,
	"DTMF":           15,
	"CALLDURATION":   16,
	"CAUSE":          17,
	"RLGTIME":        18,
	"ALERTTIME":      19,
	"CONNECTTIME":    20,
	"DISCONNECTTIME": 21,
	"SID":            22,
	"IDFLAG":         23,
	"RAWRELATENUM":   24,
	"REDIRFLAG":      25,
	"ORIGCALLEDNO":   26,
	"DISCONNECTTYPE": 27,
	"NEWLAI":         28,
	"NEWCI":          29,
	"NEWLONGITUDE":   30,
	"NEWLATITUDE":    31,
	"TMSI":           32,
	"SPCODE":         33,
	"VOICEFLAG":      34,
	"VOICEKEYA":      35,
	"VOICEKEYB":      36,
	"PEERSID":        37,
}

func cdrParseRecord(str string) (*Record, error) {
	fields := strings.Split(str, ",")
	if len(fields) != 38 {
		return nil, fmt.Errorf("wrong column count, expect 38, actual %d", len(fields))
	}
	return parseRecord(fields, g_cdrFields)
}

var g_smsFields = map[string]uint8{
	"MSISDN":       1,
	"HOMEAREA":     2,
	"RELATENUM":    3,
	"RELATEHOMEAC": 4,
	"IMSI":         5,
	"IMEI":         6,
	"CURAREA":      7,
	"NEID":         8,
	"LAI":          9,
	"CI":           10,
	"LONGITUDE":    11,
	"LATITUDE":     12,
	"CALLTYPE":     13,
	"SENDTIME":     14,
	"MESSAGE":      15,
	"SID":          16,
	"MSGTAG":       17,
	"IDFLAG":       18,
	"RAWRELATENUM": 19,
	"TMSI":         20,
	"SPCODE":       21,
}

func smsParseRecord(str string) (*Record, error) {
	fields := strings.Split(str, "${sp}")
	if len(fields) != 22 {
		return nil, fmt.Errorf("wrong column count, expect 22, actual %d", len(fields))
	}
	return parseRecord(fields, g_smsFields)
}

var g_evtFields = map[string]uint8{
	"CALLTYPE":     1,
	"MSISDN":       2,
	"HOMEAREA":     3,
	"RELATENUM":    4,
	"RELATEHOMEAC": 5,
	"IMSI":         6,
	"IMEI":         7,
	"CURAREA":      8,
	"NEID":         9,
	"LAI":          10,
	"CI":           11,
	"LONGITUDE":    12,
	"LATITUDE":     13,
	"OLDLAI":       14,
	"OLDCI":        15,
	"OLDLONGITUDE": 16,
	"OLDLATITUDE":  17,
	"SID":          18,
	"STATED":       19,
	"IDFLAG":       20,
	"DTMF":         21,
	"TMSI":         22,
	"SPCODE":       23,
}

func evtParseRecord(str string) (*Record, error) {
	fields := strings.Split(str, ",")
	if len(fields) != 24 {
		return nil, fmt.Errorf("wrong column count, expect 24, actual %d", len(fields))
	}
	return parseRecord(fields, g_evtFields)
}
