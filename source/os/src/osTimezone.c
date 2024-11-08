/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the GNU Affero General Public License, version 3
 * or later ("AGPL"), as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

#define ALLOW_FORBID_FUNC
#define _DEFAULT_SOURCE
#include "os.h"

#ifdef WINDOWS
#if (_WIN64)
#include <iphlpapi.h>
#include <mswsock.h>
#include <psapi.h>
#include <stdio.h>
#include <windows.h>
#include <ws2tcpip.h>
#pragma comment(lib, "Mswsock.lib ")
#endif
#include <objbase.h>
#pragma warning(push)
#pragma warning(disable : 4091)
#include <DbgHelp.h>
#pragma warning(pop)

char *win_tz[139][2] = {{"China Standard Time", "Asia/Shanghai"},
                        {"AUS Central Standard Time", "Australia/Darwin"},
                        {"AUS Eastern Standard Time", "Australia/Sydney"},
                        {"Afghanistan Standard Time", "Asia/Kabul"},
                        {"Alaskan Standard Time", "America/Anchorage"},
                        {"Aleutian Standard Time", "America/Adak"},
                        {"Altai Standard Time", "Asia/Barnaul"},
                        {"Arab Standard Time", "Asia/Riyadh"},
                        {"Arabian Standard Time", "Asia/Dubai"},
                        {"Arabic Standard Time", "Asia/Baghdad"},
                        {"Argentina Standard Time", "America/Buenos_Aires"},
                        {"Astrakhan Standard Time", "Europe/Astrakhan"},
                        {"Atlantic Standard Time", "America/Halifax"},
                        {"Aus Central W. Standard Time", "Australia/Eucla"},
                        {"Azerbaijan Standard Time", "Asia/Baku"},
                        {"Azores Standard Time", "Atlantic/Azores"},
                        {"Bahia Standard Time", "America/Bahia"},
                        {"Bangladesh Standard Time", "Asia/Dhaka"},
                        {"Belarus Standard Time", "Europe/Minsk"},
                        {"Bougainville Standard Time", "Pacific/Bougainville"},
                        {"Canada Central Standard Time", "America/Regina"},
                        {"Cape Verde Standard Time", "Atlantic/Cape_Verde"},
                        {"Caucasus Standard Time", "Asia/Yerevan"},
                        {"Cen. Australia Standard Time", "Australia/Adelaide"},
                        {"Central America Standard Time", "America/Guatemala"},
                        {"Central Asia Standard Time", "Asia/Almaty"},
                        {"Central Brazilian Standard Time", "America/Cuiaba"},
                        {"Central Europe Standard Time", "Europe/Budapest"},
                        {"Central European Standard Time", "Europe/Warsaw"},
                        {"Central Pacific Standard Time", "Pacific/Guadalcanal"},
                        {"Central Standard Time", "America/Chicago"},
                        {"Central Standard Time (Mexico)", "America/Mexico_City"},
                        {"Chatham Islands Standard Time", "Pacific/Chatham"},
                        {"Cuba Standard Time", "America/Havana"},
                        {"Dateline Standard Time", "Etc/GMT+12"},
                        {"E. Africa Standard Time", "Africa/Nairobi"},
                        {"E. Australia Standard Time", "Australia/Brisbane"},
                        {"E. Europe Standard Time", "Europe/Chisinau"},
                        {"E. South America Standard Time", "America/Sao_Paulo"},
                        {"Easter Island Standard Time", "Pacific/Easter"},
                        {"Eastern Standard Time", "America/New_York"},
                        {"Eastern Standard Time (Mexico)", "America/Cancun"},
                        {"Egypt Standard Time", "Africa/Cairo"},
                        {"Ekaterinburg Standard Time", "Asia/Yekaterinburg"},
                        {"FLE Standard Time", "Europe/Kiev"},
                        {"Fiji Standard Time", "Pacific/Fiji"},
                        {"GMT Standard Time", "Europe/London"},
                        {"GTB Standard Time", "Europe/Bucharest"},
                        {"Georgian Standard Time", "Asia/Tbilisi"},
                        {"Greenland Standard Time", "America/Godthab"},
                        {"Greenwich Standard Time", "Atlantic/Reykjavik"},
                        {"Haiti Standard Time", "America/Port-au-Prince"},
                        {"Hawaiian Standard Time", "Pacific/Honolulu"},
                        {"India Standard Time", "Asia/Calcutta"},
                        {"Iran Standard Time", "Asia/Tehran"},
                        {"Israel Standard Time", "Asia/Jerusalem"},
                        {"Jordan Standard Time", "Asia/Amman"},
                        {"Kaliningrad Standard Time", "Europe/Kaliningrad"},
                        {"Korea Standard Time", "Asia/Seoul"},
                        {"Libya Standard Time", "Africa/Tripoli"},
                        {"Line Islands Standard Time", "Pacific/Kiritimati"},
                        {"Lord Howe Standard Time", "Australia/Lord_Howe"},
                        {"Magadan Standard Time", "Asia/Magadan"},
                        {"Magallanes Standard Time", "America/Punta_Arenas"},
                        {"Marquesas Standard Time", "Pacific/Marquesas"},
                        {"Mauritius Standard Time", "Indian/Mauritius"},
                        {"Middle East Standard Time", "Asia/Beirut"},
                        {"Montevideo Standard Time", "America/Montevideo"},
                        {"Morocco Standard Time", "Africa/Casablanca"},
                        {"Mountain Standard Time", "America/Denver"},
                        {"Mountain Standard Time (Mexico)", "America/Chihuahua"},
                        {"Myanmar Standard Time", "Asia/Rangoon"},
                        {"N. Central Asia Standard Time", "Asia/Novosibirsk"},
                        {"Namibia Standard Time", "Africa/Windhoek"},
                        {"Nepal Standard Time", "Asia/Katmandu"},
                        {"New Zealand Standard Time", "Pacific/Auckland"},
                        {"Newfoundland Standard Time", "America/St_Johns"},
                        {"Norfolk Standard Time", "Pacific/Norfolk"},
                        {"North Asia East Standard Time", "Asia/Irkutsk"},
                        {"North Asia Standard Time", "Asia/Krasnoyarsk"},
                        {"North Korea Standard Time", "Asia/Pyongyang"},
                        {"Omsk Standard Time", "Asia/Omsk"},
                        {"Pacific SA Standard Time", "America/Santiago"},
                        {"Pacific Standard Time", "America/Los_Angeles"},
                        {"Pacific Standard Time (Mexico)", "America/Tijuana"},
                        {"Pakistan Standard Time", "Asia/Karachi"},
                        {"Paraguay Standard Time", "America/Asuncion"},
                        {"Qyzylorda Standard Time", "Asia/Qyzylorda"},
                        {"Romance Standard Time", "Europe/Paris"},
                        {"Russia Time Zone 10", "Asia/Srednekolymsk"},
                        {"Russia Time Zone 11", "Asia/Kamchatka"},
                        {"Russia Time Zone 3", "Europe/Samara"},
                        {"Russian Standard Time", "Europe/Moscow"},
                        {"SA Eastern Standard Time", "America/Cayenne"},
                        {"SA Pacific Standard Time", "America/Bogota"},
                        {"SA Western Standard Time", "America/La_Paz"},
                        {"SE Asia Standard Time", "Asia/Bangkok"},
                        {"Saint Pierre Standard Time", "America/Miquelon"},
                        {"Sakhalin Standard Time", "Asia/Sakhalin"},
                        {"Samoa Standard Time", "Pacific/Apia"},
                        {"Sao Tome Standard Time", "Africa/Sao_Tome"},
                        {"Saratov Standard Time", "Europe/Saratov"},
                        {"Singapore Standard Time", "Asia/Singapore"},
                        {"South Africa Standard Time", "Africa/Johannesburg"},
                        {"South Sudan Standard Time", "Africa/Juba"},
                        {"Sri Lanka Standard Time", "Asia/Colombo"},
                        {"Sudan Standard Time", "Africa/Khartoum"},
                        {"Syria Standard Time", "Asia/Damascus"},
                        {"Taipei Standard Time", "Asia/Taipei"},
                        {"Tasmania Standard Time", "Australia/Hobart"},
                        {"Tocantins Standard Time", "America/Araguaina"},
                        {"Tokyo Standard Time", "Asia/Tokyo"},
                        {"Tomsk Standard Time", "Asia/Tomsk"},
                        {"Tonga Standard Time", "Pacific/Tongatapu"},
                        {"Transbaikal Standard Time", "Asia/Chita"},
                        {"Turkey Standard Time", "Europe/Istanbul"},
                        {"Turks And Caicos Standard Time", "America/Grand_Turk"},
                        {"US Eastern Standard Time", "America/Indianapolis"},
                        {"US Mountain Standard Time", "America/Phoenix"},
                        {"UTC", "Etc/UTC"},
                        {"UTC+12", "Etc/GMT-12"},
                        {"UTC+13", "Etc/GMT-13"},
                        {"UTC-02", "Etc/GMT+2"},
                        {"UTC-08", "Etc/GMT+8"},
                        {"UTC-09", "Etc/GMT+9"},
                        {"UTC-11", "Etc/GMT+11"},
                        {"Ulaanbaatar Standard Time", "Asia/Ulaanbaatar"},
                        {"Venezuela Standard Time", "America/Caracas"},
                        {"Vladivostok Standard Time", "Asia/Vladivostok"},
                        {"Volgograd Standard Time", "Europe/Volgograd"},
                        {"W. Australia Standard Time", "Australia/Perth"},
                        {"W. Central Africa Standard Time", "Africa/Lagos"},
                        {"W. Europe Standard Time", "Europe/Berlin"},
                        {"W. Mongolia Standard Time", "Asia/Hovd"},
                        {"West Asia Standard Time", "Asia/Tashkent"},
                        {"West Bank Standard Time", "Asia/Hebron"},
                        {"West Pacific Standard Time", "Pacific/Port_Moresby"},
                        {"Yakutsk Standard Time", "Asia/Yakutsk"},
                        {"Yukon Standard Time", "America/Whitehorse"}};
char *tz_win[554][2] = {{"Asia/Shanghai", "China Standard Time"},
                        {"Africa/Abidjan", "Greenwich Standard Time"},
                        {"Africa/Accra", "Greenwich Standard Time"},
                        {"Africa/Addis_Ababa", "E. Africa Standard Time"},
                        {"Africa/Algiers", "W. Central Africa Standard Time"},
                        {"Africa/Asmera", "E. Africa Standard Time"},
                        {"Africa/Bamako", "Greenwich Standard Time"},
                        {"Africa/Bangui", "W. Central Africa Standard Time"},
                        {"Africa/Banjul", "Greenwich Standard Time"},
                        {"Africa/Bissau", "Greenwich Standard Time"},
                        {"Africa/Blantyre", "South Africa Standard Time"},
                        {"Africa/Brazzaville", "W. Central Africa Standard Time"},
                        {"Africa/Bujumbura", "South Africa Standard Time"},
                        {"Africa/Cairo", "Egypt Standard Time"},
                        {"Africa/Casablanca", "Morocco Standard Time"},
                        {"Africa/Ceuta", "Romance Standard Time"},
                        {"Africa/Conakry", "Greenwich Standard Time"},
                        {"Africa/Dakar", "Greenwich Standard Time"},
                        {"Africa/Dar_es_Salaam", "E. Africa Standard Time"},
                        {"Africa/Djibouti", "E. Africa Standard Time"},
                        {"Africa/Douala", "W. Central Africa Standard Time"},
                        {"Africa/El_Aaiun", "Morocco Standard Time"},
                        {"Africa/Freetown", "Greenwich Standard Time"},
                        {"Africa/Gaborone", "South Africa Standard Time"},
                        {"Africa/Harare", "South Africa Standard Time"},
                        {"Africa/Johannesburg", "South Africa Standard Time"},
                        {"Africa/Juba", "South Sudan Standard Time"},
                        {"Africa/Kampala", "E. Africa Standard Time"},
                        {"Africa/Khartoum", "Sudan Standard Time"},
                        {"Africa/Kigali", "South Africa Standard Time"},
                        {"Africa/Kinshasa", "W. Central Africa Standard Time"},
                        {"Africa/Lagos", "W. Central Africa Standard Time"},
                        {"Africa/Libreville", "W. Central Africa Standard Time"},
                        {"Africa/Lome", "Greenwich Standard Time"},
                        {"Africa/Luanda", "W. Central Africa Standard Time"},
                        {"Africa/Lubumbashi", "South Africa Standard Time"},
                        {"Africa/Lusaka", "South Africa Standard Time"},
                        {"Africa/Malabo", "W. Central Africa Standard Time"},
                        {"Africa/Maputo", "South Africa Standard Time"},
                        {"Africa/Maseru", "South Africa Standard Time"},
                        {"Africa/Mbabane", "South Africa Standard Time"},
                        {"Africa/Mogadishu", "E. Africa Standard Time"},
                        {"Africa/Monrovia", "Greenwich Standard Time"},
                        {"Africa/Nairobi", "E. Africa Standard Time"},
                        {"Africa/Ndjamena", "W. Central Africa Standard Time"},
                        {"Africa/Niamey", "W. Central Africa Standard Time"},
                        {"Africa/Nouakchott", "Greenwich Standard Time"},
                        {"Africa/Ouagadougou", "Greenwich Standard Time"},
                        {"Africa/Porto-Novo", "W. Central Africa Standard Time"},
                        {"Africa/Sao_Tome", "Sao Tome Standard Time"},
                        {"Africa/Timbuktu", "Greenwich Standard Time"},
                        {"Africa/Tripoli", "Libya Standard Time"},
                        {"Africa/Tunis", "W. Central Africa Standard Time"},
                        {"Africa/Windhoek", "Namibia Standard Time"},
                        {"America/Adak", "Aleutian Standard Time"},
                        {"America/Anchorage", "Alaskan Standard Time"},
                        {"America/Anguilla", "SA Western Standard Time"},
                        {"America/Antigua", "SA Western Standard Time"},
                        {"America/Araguaina", "Tocantins Standard Time"},
                        {"America/Argentina/La_Rioja", "Argentina Standard Time"},
                        {"America/Argentina/Rio_Gallegos", "Argentina Standard Time"},
                        {"America/Argentina/Salta", "Argentina Standard Time"},
                        {"America/Argentina/San_Juan", "Argentina Standard Time"},
                        {"America/Argentina/San_Luis", "Argentina Standard Time"},
                        {"America/Argentina/Tucuman", "Argentina Standard Time"},
                        {"America/Argentina/Ushuaia", "Argentina Standard Time"},
                        {"America/Aruba", "SA Western Standard Time"},
                        {"America/Asuncion", "Paraguay Standard Time"},
                        {"America/Atka", "Aleutian Standard Time"},
                        {"America/Bahia", "Bahia Standard Time"},
                        {"America/Bahia_Banderas", "Central Standard Time (Mexico)"},
                        {"America/Barbados", "SA Western Standard Time"},
                        {"America/Belem", "SA Eastern Standard Time"},
                        {"America/Belize", "Central America Standard Time"},
                        {"America/Blanc-Sablon", "SA Western Standard Time"},
                        {"America/Boa_Vista", "SA Western Standard Time"},
                        {"America/Bogota", "SA Pacific Standard Time"},
                        {"America/Boise", "Mountain Standard Time"},
                        {"America/Buenos_Aires", "Argentina Standard Time"},
                        {"America/Cambridge_Bay", "Mountain Standard Time"},
                        {"America/Campo_Grande", "Central Brazilian Standard Time"},
                        {"America/Cancun", "Eastern Standard Time (Mexico)"},
                        {"America/Caracas", "Venezuela Standard Time"},
                        {"America/Catamarca", "Argentina Standard Time"},
                        {"America/Cayenne", "SA Eastern Standard Time"},
                        {"America/Cayman", "SA Pacific Standard Time"},
                        {"America/Chicago", "Central Standard Time"},
                        {"America/Chihuahua", "Mountain Standard Time (Mexico)"},
                        {"America/Coral_Harbour", "SA Pacific Standard Time"},
                        {"America/Cordoba", "Argentina Standard Time"},
                        {"America/Costa_Rica", "Central America Standard Time"},
                        {"America/Creston", "US Mountain Standard Time"},
                        {"America/Cuiaba", "Central Brazilian Standard Time"},
                        {"America/Curacao", "SA Western Standard Time"},
                        {"America/Danmarkshavn", "Greenwich Standard Time"},
                        {"America/Dawson", "Yukon Standard Time"},
                        {"America/Dawson_Creek", "US Mountain Standard Time"},
                        {"America/Denver", "Mountain Standard Time"},
                        {"America/Detroit", "Eastern Standard Time"},
                        {"America/Dominica", "SA Western Standard Time"},
                        {"America/Edmonton", "Mountain Standard Time"},
                        {"America/Eirunepe", "SA Pacific Standard Time"},
                        {"America/El_Salvador", "Central America Standard Time"},
                        {"America/Ensenada", "Pacific Standard Time (Mexico)"},
                        {"America/Fort_Nelson", "US Mountain Standard Time"},
                        {"America/Fortaleza", "SA Eastern Standard Time"},
                        {"America/Glace_Bay", "Atlantic Standard Time"},
                        {"America/Godthab", "Greenland Standard Time"},
                        {"America/Goose_Bay", "Atlantic Standard Time"},
                        {"America/Grand_Turk", "Turks And Caicos Standard Time"},
                        {"America/Grenada", "SA Western Standard Time"},
                        {"America/Guadeloupe", "SA Western Standard Time"},
                        {"America/Guatemala", "Central America Standard Time"},
                        {"America/Guayaquil", "SA Pacific Standard Time"},
                        {"America/Guyana", "SA Western Standard Time"},
                        {"America/Halifax", "Atlantic Standard Time"},
                        {"America/Havana", "Cuba Standard Time"},
                        {"America/Hermosillo", "US Mountain Standard Time"},
                        {"America/Indiana/Knox", "Central Standard Time"},
                        {"America/Indiana/Marengo", "US Eastern Standard Time"},
                        {"America/Indiana/Petersburg", "Eastern Standard Time"},
                        {"America/Indiana/Tell_City", "Central Standard Time"},
                        {"America/Indiana/Vevay", "US Eastern Standard Time"},
                        {"America/Indiana/Vincennes", "Eastern Standard Time"},
                        {"America/Indiana/Winamac", "Eastern Standard Time"},
                        {"America/Indianapolis", "US Eastern Standard Time"},
                        {"America/Inuvik", "Mountain Standard Time"},
                        {"America/Iqaluit", "Eastern Standard Time"},
                        {"America/Jamaica", "SA Pacific Standard Time"},
                        {"America/Jujuy", "Argentina Standard Time"},
                        {"America/Juneau", "Alaskan Standard Time"},
                        {"America/Kentucky/Monticello", "Eastern Standard Time"},
                        {"America/Knox_IN", "Central Standard Time"},
                        {"America/Kralendijk", "SA Western Standard Time"},
                        {"America/La_Paz", "SA Western Standard Time"},
                        {"America/Lima", "SA Pacific Standard Time"},
                        {"America/Los_Angeles", "Pacific Standard Time"},
                        {"America/Louisville", "Eastern Standard Time"},
                        {"America/Lower_Princes", "SA Western Standard Time"},
                        {"America/Maceio", "SA Eastern Standard Time"},
                        {"America/Managua", "Central America Standard Time"},
                        {"America/Manaus", "SA Western Standard Time"},
                        {"America/Marigot", "SA Western Standard Time"},
                        {"America/Martinique", "SA Western Standard Time"},
                        {"America/Matamoros", "Central Standard Time"},
                        {"America/Mazatlan", "Mountain Standard Time (Mexico)"},
                        {"America/Mendoza", "Argentina Standard Time"},
                        {"America/Menominee", "Central Standard Time"},
                        {"America/Merida", "Central Standard Time (Mexico)"},
                        {"America/Metlakatla", "Alaskan Standard Time"},
                        {"America/Mexico_City", "Central Standard Time (Mexico)"},
                        {"America/Miquelon", "Saint Pierre Standard Time"},
                        {"America/Moncton", "Atlantic Standard Time"},
                        {"America/Monterrey", "Central Standard Time (Mexico)"},
                        {"America/Montevideo", "Montevideo Standard Time"},
                        {"America/Montreal", "Eastern Standard Time"},
                        {"America/Montserrat", "SA Western Standard Time"},
                        {"America/Nassau", "Eastern Standard Time"},
                        {"America/New_York", "Eastern Standard Time"},
                        {"America/Nipigon", "Eastern Standard Time"},
                        {"America/Nome", "Alaskan Standard Time"},
                        {"America/Noronha", "UTC-02"},
                        {"America/North_Dakota/Beulah", "Central Standard Time"},
                        {"America/North_Dakota/Center", "Central Standard Time"},
                        {"America/North_Dakota/New_Salem", "Central Standard Time"},
                        {"America/Ojinaga", "Mountain Standard Time"},
                        {"America/Panama", "SA Pacific Standard Time"},
                        {"America/Pangnirtung", "Eastern Standard Time"},
                        {"America/Paramaribo", "SA Eastern Standard Time"},
                        {"America/Phoenix", "US Mountain Standard Time"},
                        {"America/Port-au-Prince", "Haiti Standard Time"},
                        {"America/Port_of_Spain", "SA Western Standard Time"},
                        {"America/Porto_Acre", "SA Pacific Standard Time"},
                        {"America/Porto_Velho", "SA Western Standard Time"},
                        {"America/Puerto_Rico", "SA Western Standard Time"},
                        {"America/Punta_Arenas", "Magallanes Standard Time"},
                        {"America/Rainy_River", "Central Standard Time"},
                        {"America/Rankin_Inlet", "Central Standard Time"},
                        {"America/Recife", "SA Eastern Standard Time"},
                        {"America/Regina", "Canada Central Standard Time"},
                        {"America/Resolute", "Central Standard Time"},
                        {"America/Rio_Branco", "SA Pacific Standard Time"},
                        {"America/Santa_Isabel", "Pacific Standard Time (Mexico)"},
                        {"America/Santarem", "SA Eastern Standard Time"},
                        {"America/Santiago", "Pacific SA Standard Time"},
                        {"America/Santo_Domingo", "SA Western Standard Time"},
                        {"America/Sao_Paulo", "E. South America Standard Time"},
                        {"America/Scoresbysund", "Azores Standard Time"},
                        {"America/Shiprock", "Mountain Standard Time"},
                        {"America/Sitka", "Alaskan Standard Time"},
                        {"America/St_Barthelemy", "SA Western Standard Time"},
                        {"America/St_Johns", "Newfoundland Standard Time"},
                        {"America/St_Kitts", "SA Western Standard Time"},
                        {"America/St_Lucia", "SA Western Standard Time"},
                        {"America/St_Thomas", "SA Western Standard Time"},
                        {"America/St_Vincent", "SA Western Standard Time"},
                        {"America/Swift_Current", "Canada Central Standard Time"},
                        {"America/Tegucigalpa", "Central America Standard Time"},
                        {"America/Thule", "Atlantic Standard Time"},
                        {"America/Thunder_Bay", "Eastern Standard Time"},
                        {"America/Tijuana", "Pacific Standard Time (Mexico)"},
                        {"America/Toronto", "Eastern Standard Time"},
                        {"America/Tortola", "SA Western Standard Time"},
                        {"America/Vancouver", "Pacific Standard Time"},
                        {"America/Virgin", "SA Western Standard Time"},
                        {"America/Whitehorse", "Yukon Standard Time"},
                        {"America/Winnipeg", "Central Standard Time"},
                        {"America/Yakutat", "Alaskan Standard Time"},
                        {"America/Yellowknife", "Mountain Standard Time"},
                        {"Antarctica/Casey", "Central Pacific Standard Time"},
                        {"Antarctica/Davis", "SE Asia Standard Time"},
                        {"Antarctica/DumontDUrville", "West Pacific Standard Time"},
                        {"Antarctica/Macquarie", "Tasmania Standard Time"},
                        {"Antarctica/Mawson", "West Asia Standard Time"},
                        {"Antarctica/McMurdo", "New Zealand Standard Time"},
                        {"Antarctica/Palmer", "SA Eastern Standard Time"},
                        {"Antarctica/Rothera", "SA Eastern Standard Time"},
                        {"Antarctica/South_Pole", "New Zealand Standard Time"},
                        {"Antarctica/Syowa", "E. Africa Standard Time"},
                        {"Antarctica/Vostok", "Central Asia Standard Time"},
                        {"Arctic/Longyearbyen", "W. Europe Standard Time"},
                        {"Asia/Aden", "Arab Standard Time"},
                        {"Asia/Almaty", "Central Asia Standard Time"},
                        {"Asia/Amman", "Jordan Standard Time"},
                        {"Asia/Anadyr", "Russia Time Zone 11"},
                        {"Asia/Aqtau", "West Asia Standard Time"},
                        {"Asia/Aqtobe", "West Asia Standard Time"},
                        {"Asia/Ashgabat", "West Asia Standard Time"},
                        {"Asia/Ashkhabad", "West Asia Standard Time"},
                        {"Asia/Atyrau", "West Asia Standard Time"},
                        {"Asia/Baghdad", "Arabic Standard Time"},
                        {"Asia/Bahrain", "Arab Standard Time"},
                        {"Asia/Baku", "Azerbaijan Standard Time"},
                        {"Asia/Bangkok", "SE Asia Standard Time"},
                        {"Asia/Barnaul", "Altai Standard Time"},
                        {"Asia/Beirut", "Middle East Standard Time"},
                        {"Asia/Bishkek", "Central Asia Standard Time"},
                        {"Asia/Brunei", "Singapore Standard Time"},
                        {"Asia/Calcutta", "India Standard Time"},
                        {"Asia/Chita", "Transbaikal Standard Time"},
                        {"Asia/Choibalsan", "Ulaanbaatar Standard Time"},
                        {"Asia/Chongqing", "China Standard Time"},
                        {"Asia/Chungking", "China Standard Time"},
                        {"Asia/Colombo", "Sri Lanka Standard Time"},
                        {"Asia/Dacca", "Bangladesh Standard Time"},
                        {"Asia/Damascus", "Syria Standard Time"},
                        {"Asia/Dhaka", "Bangladesh Standard Time"},
                        {"Asia/Dili", "Tokyo Standard Time"},
                        {"Asia/Dubai", "Arabian Standard Time"},
                        {"Asia/Dushanbe", "West Asia Standard Time"},
                        {"Asia/Famagusta", "GTB Standard Time"},
                        {"Asia/Gaza", "West Bank Standard Time"},
                        {"Asia/Harbin", "China Standard Time"},
                        {"Asia/Hebron", "West Bank Standard Time"},
                        {"Asia/Hong_Kong", "China Standard Time"},
                        {"Asia/Hovd", "W. Mongolia Standard Time"},
                        {"Asia/Irkutsk", "North Asia East Standard Time"},
                        {"Asia/Jakarta", "SE Asia Standard Time"},
                        {"Asia/Jayapura", "Tokyo Standard Time"},
                        {"Asia/Jerusalem", "Israel Standard Time"},
                        {"Asia/Kabul", "Afghanistan Standard Time"},
                        {"Asia/Kamchatka", "Russia Time Zone 11"},
                        {"Asia/Karachi", "Pakistan Standard Time"},
                        {"Asia/Kashgar", "Central Asia Standard Time"},
                        {"Asia/Katmandu", "Nepal Standard Time"},
                        {"Asia/Khandyga", "Yakutsk Standard Time"},
                        {"Asia/Krasnoyarsk", "North Asia Standard Time"},
                        {"Asia/Kuala_Lumpur", "Singapore Standard Time"},
                        {"Asia/Kuching", "Singapore Standard Time"},
                        {"Asia/Kuwait", "Arab Standard Time"},
                        {"Asia/Macao", "China Standard Time"},
                        {"Asia/Macau", "China Standard Time"},
                        {"Asia/Magadan", "Magadan Standard Time"},
                        {"Asia/Makassar", "Singapore Standard Time"},
                        {"Asia/Manila", "Singapore Standard Time"},
                        {"Asia/Muscat", "Arabian Standard Time"},
                        {"Asia/Nicosia", "GTB Standard Time"},
                        {"Asia/Novokuznetsk", "North Asia Standard Time"},
                        {"Asia/Novosibirsk", "N. Central Asia Standard Time"},
                        {"Asia/Omsk", "Omsk Standard Time"},
                        {"Asia/Oral", "West Asia Standard Time"},
                        {"Asia/Phnom_Penh", "SE Asia Standard Time"},
                        {"Asia/Pontianak", "SE Asia Standard Time"},
                        {"Asia/Pyongyang", "North Korea Standard Time"},
                        {"Asia/Qatar", "Arab Standard Time"},
                        {"Asia/Qostanay", "Central Asia Standard Time"},
                        {"Asia/Qyzylorda", "Qyzylorda Standard Time"},
                        {"Asia/Rangoon", "Myanmar Standard Time"},
                        {"Asia/Riyadh", "Arab Standard Time"},
                        {"Asia/Saigon", "SE Asia Standard Time"},
                        {"Asia/Sakhalin", "Sakhalin Standard Time"},
                        {"Asia/Samarkand", "West Asia Standard Time"},
                        {"Asia/Seoul", "Korea Standard Time"},
                        {"Asia/Singapore", "Singapore Standard Time"},
                        {"Asia/Srednekolymsk", "Russia Time Zone 10"},
                        {"Asia/Taipei", "Taipei Standard Time"},
                        {"Asia/Tashkent", "West Asia Standard Time"},
                        {"Asia/Tbilisi", "Georgian Standard Time"},
                        {"Asia/Tehran", "Iran Standard Time"},
                        {"Asia/Tel_Aviv", "Israel Standard Time"},
                        {"Asia/Thimbu", "Bangladesh Standard Time"},
                        {"Asia/Thimphu", "Bangladesh Standard Time"},
                        {"Asia/Tokyo", "Tokyo Standard Time"},
                        {"Asia/Tomsk", "Tomsk Standard Time"},
                        {"Asia/Ujung_Pandang", "Singapore Standard Time"},
                        {"Asia/Ulaanbaatar", "Ulaanbaatar Standard Time"},
                        {"Asia/Ulan_Bator", "Ulaanbaatar Standard Time"},
                        {"Asia/Urumqi", "Central Asia Standard Time"},
                        {"Asia/Ust-Nera", "Vladivostok Standard Time"},
                        {"Asia/Vientiane", "SE Asia Standard Time"},
                        {"Asia/Vladivostok", "Vladivostok Standard Time"},
                        {"Asia/Yakutsk", "Yakutsk Standard Time"},
                        {"Asia/Yekaterinburg", "Ekaterinburg Standard Time"},
                        {"Asia/Yerevan", "Caucasus Standard Time"},
                        {"Atlantic/Azores", "Azores Standard Time"},
                        {"Atlantic/Bermuda", "Atlantic Standard Time"},
                        {"Atlantic/Canary", "GMT Standard Time"},
                        {"Atlantic/Cape_Verde", "Cape Verde Standard Time"},
                        {"Atlantic/Faeroe", "GMT Standard Time"},
                        {"Atlantic/Jan_Mayen", "W. Europe Standard Time"},
                        {"Atlantic/Madeira", "GMT Standard Time"},
                        {"Atlantic/Reykjavik", "Greenwich Standard Time"},
                        {"Atlantic/South_Georgia", "UTC-02"},
                        {"Atlantic/St_Helena", "Greenwich Standard Time"},
                        {"Atlantic/Stanley", "SA Eastern Standard Time"},
                        {"Australia/ACT", "AUS Eastern Standard Time"},
                        {"Australia/Adelaide", "Cen. Australia Standard Time"},
                        {"Australia/Brisbane", "E. Australia Standard Time"},
                        {"Australia/Broken_Hill", "Cen. Australia Standard Time"},
                        {"Australia/Canberra", "AUS Eastern Standard Time"},
                        {"Australia/Currie", "Tasmania Standard Time"},
                        {"Australia/Darwin", "AUS Central Standard Time"},
                        {"Australia/Eucla", "Aus Central W. Standard Time"},
                        {"Australia/Hobart", "Tasmania Standard Time"},
                        {"Australia/LHI", "Lord Howe Standard Time"},
                        {"Australia/Lindeman", "E. Australia Standard Time"},
                        {"Australia/Lord_Howe", "Lord Howe Standard Time"},
                        {"Australia/Melbourne", "AUS Eastern Standard Time"},
                        {"Australia/NSW", "AUS Eastern Standard Time"},
                        {"Australia/North", "AUS Central Standard Time"},
                        {"Australia/Perth", "W. Australia Standard Time"},
                        {"Australia/Queensland", "E. Australia Standard Time"},
                        {"Australia/South", "Cen. Australia Standard Time"},
                        {"Australia/Sydney", "AUS Eastern Standard Time"},
                        {"Australia/Tasmania", "Tasmania Standard Time"},
                        {"Australia/Victoria", "AUS Eastern Standard Time"},
                        {"Australia/West", "W. Australia Standard Time"},
                        {"Australia/Yancowinna", "Cen. Australia Standard Time"},
                        {"Brazil/Acre", "SA Pacific Standard Time"},
                        {"Brazil/DeNoronha", "UTC-02"},
                        {"Brazil/East", "E. South America Standard Time"},
                        {"Brazil/West", "SA Western Standard Time"},
                        {"CST6CDT", "Central Standard Time"},
                        {"Canada/Atlantic", "Atlantic Standard Time"},
                        {"Canada/Central", "Central Standard Time"},
                        {"Canada/Eastern", "Eastern Standard Time"},
                        {"Canada/Mountain", "Mountain Standard Time"},
                        {"Canada/Newfoundland", "Newfoundland Standard Time"},
                        {"Canada/Pacific", "Pacific Standard Time"},
                        {"Canada/Saskatchewan", "Canada Central Standard Time"},
                        {"Canada/Yukon", "Yukon Standard Time"},
                        {"Chile/Continental", "Pacific SA Standard Time"},
                        {"Chile/EasterIsland", "Easter Island Standard Time"},
                        {"Cuba", "Cuba Standard Time"},
                        {"EST5EDT", "Eastern Standard Time"},
                        {"Egypt", "Egypt Standard Time"},
                        {"Eire", "GMT Standard Time"},
                        {"Etc/GMT", "UTC"},
                        {"Etc/GMT+1", "Cape Verde Standard Time"},
                        {"Etc/GMT+10", "Hawaiian Standard Time"},
                        {"Etc/GMT+11", "UTC-11"},
                        {"Etc/GMT+12", "Dateline Standard Time"},
                        {"Etc/GMT+2", "UTC-02"},
                        {"Etc/GMT+3", "SA Eastern Standard Time"},
                        {"Etc/GMT+4", "SA Western Standard Time"},
                        {"Etc/GMT+5", "SA Pacific Standard Time"},
                        {"Etc/GMT+6", "Central America Standard Time"},
                        {"Etc/GMT+7", "US Mountain Standard Time"},
                        {"Etc/GMT+8", "UTC-08"},
                        {"Etc/GMT+9", "UTC-09"},
                        {"Etc/GMT-1", "W. Central Africa Standard Time"},
                        {"Etc/GMT-10", "West Pacific Standard Time"},
                        {"Etc/GMT-11", "Central Pacific Standard Time"},
                        {"Etc/GMT-12", "UTC+12"},
                        {"Etc/GMT-13", "UTC+13"},
                        {"Etc/GMT-14", "Line Islands Standard Time"},
                        {"Etc/GMT-2", "South Africa Standard Time"},
                        {"Etc/GMT-3", "E. Africa Standard Time"},
                        {"Etc/GMT-4", "Arabian Standard Time"},
                        {"Etc/GMT-5", "West Asia Standard Time"},
                        {"Etc/GMT-6", "Central Asia Standard Time"},
                        {"Etc/GMT-7", "SE Asia Standard Time"},
                        {"Etc/GMT-8", "Singapore Standard Time"},
                        {"Etc/GMT-9", "Tokyo Standard Time"},
                        {"Etc/UCT", "UTC"},
                        {"Etc/UTC", "UTC"},
                        {"Europe/Amsterdam", "W. Europe Standard Time"},
                        {"Europe/Andorra", "W. Europe Standard Time"},
                        {"Europe/Astrakhan", "Astrakhan Standard Time"},
                        {"Europe/Athens", "GTB Standard Time"},
                        {"Europe/Belfast", "GMT Standard Time"},
                        {"Europe/Belgrade", "Central Europe Standard Time"},
                        {"Europe/Berlin", "W. Europe Standard Time"},
                        {"Europe/Bratislava", "Central Europe Standard Time"},
                        {"Europe/Brussels", "Romance Standard Time"},
                        {"Europe/Bucharest", "GTB Standard Time"},
                        {"Europe/Budapest", "Central Europe Standard Time"},
                        {"Europe/Busingen", "W. Europe Standard Time"},
                        {"Europe/Chisinau", "E. Europe Standard Time"},
                        {"Europe/Copenhagen", "Romance Standard Time"},
                        {"Europe/Dublin", "GMT Standard Time"},
                        {"Europe/Gibraltar", "W. Europe Standard Time"},
                        {"Europe/Guernsey", "GMT Standard Time"},
                        {"Europe/Helsinki", "FLE Standard Time"},
                        {"Europe/Isle_of_Man", "GMT Standard Time"},
                        {"Europe/Istanbul", "Turkey Standard Time"},
                        {"Europe/Jersey", "GMT Standard Time"},
                        {"Europe/Kaliningrad", "Kaliningrad Standard Time"},
                        {"Europe/Kiev", "FLE Standard Time"},
                        {"Europe/Kirov", "Russian Standard Time"},
                        {"Europe/Lisbon", "GMT Standard Time"},
                        {"Europe/Ljubljana", "Central Europe Standard Time"},
                        {"Europe/London", "GMT Standard Time"},
                        {"Europe/Luxembourg", "W. Europe Standard Time"},
                        {"Europe/Madrid", "Romance Standard Time"},
                        {"Europe/Malta", "W. Europe Standard Time"},
                        {"Europe/Mariehamn", "FLE Standard Time"},
                        {"Europe/Minsk", "Belarus Standard Time"},
                        {"Europe/Monaco", "W. Europe Standard Time"},
                        {"Europe/Moscow", "Russian Standard Time"},
                        {"Europe/Oslo", "W. Europe Standard Time"},
                        {"Europe/Paris", "Romance Standard Time"},
                        {"Europe/Podgorica", "Central Europe Standard Time"},
                        {"Europe/Prague", "Central Europe Standard Time"},
                        {"Europe/Riga", "FLE Standard Time"},
                        {"Europe/Rome", "W. Europe Standard Time"},
                        {"Europe/Samara", "Russia Time Zone 3"},
                        {"Europe/San_Marino", "W. Europe Standard Time"},
                        {"Europe/Sarajevo", "Central European Standard Time"},
                        {"Europe/Saratov", "Saratov Standard Time"},
                        {"Europe/Simferopol", "Russian Standard Time"},
                        {"Europe/Skopje", "Central European Standard Time"},
                        {"Europe/Sofia", "FLE Standard Time"},
                        {"Europe/Stockholm", "W. Europe Standard Time"},
                        {"Europe/Tallinn", "FLE Standard Time"},
                        {"Europe/Tirane", "Central Europe Standard Time"},
                        {"Europe/Tiraspol", "E. Europe Standard Time"},
                        {"Europe/Ulyanovsk", "Astrakhan Standard Time"},
                        {"Europe/Uzhgorod", "FLE Standard Time"},
                        {"Europe/Vaduz", "W. Europe Standard Time"},
                        {"Europe/Vatican", "W. Europe Standard Time"},
                        {"Europe/Vienna", "W. Europe Standard Time"},
                        {"Europe/Vilnius", "FLE Standard Time"},
                        {"Europe/Volgograd", "Volgograd Standard Time"},
                        {"Europe/Warsaw", "Central European Standard Time"},
                        {"Europe/Zagreb", "Central European Standard Time"},
                        {"Europe/Zaporozhye", "FLE Standard Time"},
                        {"Europe/Zurich", "W. Europe Standard Time"},
                        {"GB", "GMT Standard Time"},
                        {"GB-Eire", "GMT Standard Time"},
                        {"GMT+0", "UTC"},
                        {"GMT-0", "UTC"},
                        {"GMT0", "UTC"},
                        {"Greenwich", "UTC"},
                        {"Hongkong", "China Standard Time"},
                        {"Iceland", "Greenwich Standard Time"},
                        {"Indian/Antananarivo", "E. Africa Standard Time"},
                        {"Indian/Chagos", "Central Asia Standard Time"},
                        {"Indian/Christmas", "SE Asia Standard Time"},
                        {"Indian/Cocos", "Myanmar Standard Time"},
                        {"Indian/Comoro", "E. Africa Standard Time"},
                        {"Indian/Kerguelen", "West Asia Standard Time"},
                        {"Indian/Mahe", "Mauritius Standard Time"},
                        {"Indian/Maldives", "West Asia Standard Time"},
                        {"Indian/Mauritius", "Mauritius Standard Time"},
                        {"Indian/Mayotte", "E. Africa Standard Time"},
                        {"Indian/Reunion", "Mauritius Standard Time"},
                        {"Iran", "Iran Standard Time"},
                        {"Israel", "Israel Standard Time"},
                        {"Jamaica", "SA Pacific Standard Time"},
                        {"Japan", "Tokyo Standard Time"},
                        {"Kwajalein", "UTC+12"},
                        {"Libya", "Libya Standard Time"},
                        {"MST7MDT", "Mountain Standard Time"},
                        {"Mexico/BajaNorte", "Pacific Standard Time (Mexico)"},
                        {"Mexico/BajaSur", "Mountain Standard Time (Mexico)"},
                        {"Mexico/General", "Central Standard Time (Mexico)"},
                        {"NZ", "New Zealand Standard Time"},
                        {"NZ-CHAT", "Chatham Islands Standard Time"},
                        {"Navajo", "Mountain Standard Time"},
                        {"PRC", "China Standard Time"},
                        {"PST8PDT", "Pacific Standard Time"},
                        {"Pacific/Apia", "Samoa Standard Time"},
                        {"Pacific/Auckland", "New Zealand Standard Time"},
                        {"Pacific/Bougainville", "Bougainville Standard Time"},
                        {"Pacific/Chatham", "Chatham Islands Standard Time"},
                        {"Pacific/Easter", "Easter Island Standard Time"},
                        {"Pacific/Efate", "Central Pacific Standard Time"},
                        {"Pacific/Enderbury", "UTC+13"},
                        {"Pacific/Fakaofo", "UTC+13"},
                        {"Pacific/Fiji", "Fiji Standard Time"},
                        {"Pacific/Funafuti", "UTC+12"},
                        {"Pacific/Galapagos", "Central America Standard Time"},
                        {"Pacific/Gambier", "UTC-09"},
                        {"Pacific/Guadalcanal", "Central Pacific Standard Time"},
                        {"Pacific/Guam", "West Pacific Standard Time"},
                        {"Pacific/Honolulu", "Hawaiian Standard Time"},
                        {"Pacific/Johnston", "Hawaiian Standard Time"},
                        {"Pacific/Kiritimati", "Line Islands Standard Time"},
                        {"Pacific/Kosrae", "Central Pacific Standard Time"},
                        {"Pacific/Kwajalein", "UTC+12"},
                        {"Pacific/Majuro", "UTC+12"},
                        {"Pacific/Marquesas", "Marquesas Standard Time"},
                        {"Pacific/Midway", "UTC-11"},
                        {"Pacific/Nauru", "UTC+12"},
                        {"Pacific/Niue", "UTC-11"},
                        {"Pacific/Norfolk", "Norfolk Standard Time"},
                        {"Pacific/Noumea", "Central Pacific Standard Time"},
                        {"Pacific/Pago_Pago", "UTC-11"},
                        {"Pacific/Palau", "Tokyo Standard Time"},
                        {"Pacific/Pitcairn", "UTC-08"},
                        {"Pacific/Ponape", "Central Pacific Standard Time"},
                        {"Pacific/Port_Moresby", "West Pacific Standard Time"},
                        {"Pacific/Rarotonga", "Hawaiian Standard Time"},
                        {"Pacific/Saipan", "West Pacific Standard Time"},
                        {"Pacific/Samoa", "UTC-11"},
                        {"Pacific/Tahiti", "Hawaiian Standard Time"},
                        {"Pacific/Tarawa", "UTC+12"},
                        {"Pacific/Tongatapu", "Tonga Standard Time"},
                        {"Pacific/Truk", "West Pacific Standard Time"},
                        {"Pacific/Wake", "UTC+12"},
                        {"Pacific/Wallis", "UTC+12"},
                        {"Poland", "Central European Standard Time"},
                        {"Portugal", "GMT Standard Time"},
                        {"ROC", "Taipei Standard Time"},
                        {"ROK", "Korea Standard Time"},
                        {"Singapore", "Singapore Standard Time"},
                        {"Turkey", "Turkey Standard Time"},
                        {"UCT", "UTC"},
                        {"US/Alaska", "Alaskan Standard Time"},
                        {"US/Aleutian", "Aleutian Standard Time"},
                        {"US/Arizona", "US Mountain Standard Time"},
                        {"US/Central", "Central Standard Time"},
                        {"US/Eastern", "Eastern Standard Time"},
                        {"US/Hawaii", "Hawaiian Standard Time"},
                        {"US/Indiana-Starke", "Central Standard Time"},
                        {"US/Michigan", "Eastern Standard Time"},
                        {"US/Mountain", "Mountain Standard Time"},
                        {"US/Pacific", "Pacific Standard Time"},
                        {"US/Samoa", "UTC-11"},
                        {"UTC", "UTC"},
                        {"Universal", "UTC"},
                        {"W-SU", "Russian Standard Time"},
                        {"Zulu", "UTC"}};
#elif defined(_TD_DARWIN_64)
#include <errno.h>
#include <libproc.h>
#else
#include <argp.h>
#include <linux/sysctl.h>
#include <sys/file.h>
#include <sys/resource.h>
#include <sys/statvfs.h>
#include <sys/syscall.h>
#include <sys/utsname.h>
#include <unistd.h>
#endif

static int isdst_now = 0;

void parseTimeStr(char *p, char to[5]) {
  for (int i = 0; i < 5; ++i) {
    if (strlen(p) > i) {
      to[i] = p[i];
    } else {
      to[i] = '0';
    }
  }
  if (strlen(p) == 2) {
    to[1] = '0';
    to[2] = p[1];
  }
}

int32_t taosSetSystemTimezone(const char *inTimezoneStr, char *outTimezoneStr, int8_t *outDaylight,
                           enum TdTimezone *tsTimezone) {
  if (inTimezoneStr == NULL || inTimezoneStr[0] == 0) {
    terrno = TSDB_CODE_INVALID_PARA;
    return terrno;
  }

  int32_t code = TSDB_CODE_SUCCESS;
  size_t len = strlen(inTimezoneStr);
  if (len >= TD_TIMEZONE_LEN) {
    terrno = TSDB_CODE_INVALID_PARA;
    return terrno;
  }
  char buf[TD_TIMEZONE_LEN] = {0};
  for (int32_t i = 0; i < len; i++) {
    if (inTimezoneStr[i] == ' ' || inTimezoneStr[i] == '(') {
      buf[i] = 0;
      break;
    }
    buf[i] = inTimezoneStr[i];
  }

#ifdef WINDOWS
  char winStr[TD_LOCALE_LEN * 2];
  memset(winStr, 0, sizeof(winStr));
  for (size_t i = 0; i < 554; i++) {
    if (strcmp(tz_win[i][0], buf) == 0) {
      char  keyPath[256];
      char  keyValue[100];
      DWORD keyValueSize = sizeof(keyValue);
      snprintf(keyPath, sizeof(keyPath), "SOFTWARE\\Microsoft\\Windows NT\\CurrentVersion\\Time Zones\\%s", tz_win[i][1]);
      RegGetValue(HKEY_LOCAL_MACHINE, keyPath, "Display", RRF_RT_ANY, NULL, (PVOID)&keyValue, &keyValueSize);
      if (keyValueSize > 0) {
        keyValue[4] = (keyValue[4] == '+' ? '-' : '+');
        keyValue[10] = 0;
        snprintf(winStr, sizeof(winStr), "TZ=%s:00", &(keyValue[1]));
        *tsTimezone = -taosStr2Int32(&keyValue[4], NULL, 10);
      }
      break;
    }
  }
  if (winStr[0] == 0) {
    char *p = strchr(inTimezoneStr, '+');
    if (p == NULL) p = strchr(inTimezoneStr, '-');
    if (p != NULL) {
      char *pp = strchr(inTimezoneStr, '(');
      char *ppp = strchr(inTimezoneStr, ',');
      int   indexStr;
      if (pp == NULL || ppp == NULL) {
        indexStr = tsnprintf(winStr, sizeof(winStr), "TZ=UTC");
      } else {
        memcpy(winStr, "TZ=", 3);
        pp++;
        memcpy(&winStr[3], pp, ppp - pp);
        indexStr = ppp - pp + 3;
      }
      char to[5];
      parseTimeStr(p, to);
      snprintf(&winStr[indexStr], sizeof(winStr) - indexStr, "%c%c%c:%c%c:00", (to[0] == '+' ? '+' : '-'), to[1], to[2], to[3], to[4]);
      *tsTimezone = -taosStr2Int32(p, NULL, 10);
    } else {
      *tsTimezone = 0;
    }
  }
  _putenv(winStr);
  _tzset();
  if (outTimezoneStr != inTimezoneStr) {
    tstrncpy(outTimezoneStr, inTimezoneStr, TD_TIMEZONE_LEN);
  }
  *outDaylight = 0;

#elif defined(_TD_DARWIN_64)

  code = setenv("TZ", buf, 1);
  if (-1 == code) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    return terrno;
  }
  tzset();
  int32_t tz = (int32_t)((-timezone * MILLISECOND_PER_SECOND) / MILLISECOND_PER_HOUR);
  *tsTimezone = tz;
  tz += isdst_now;

  snprintf(outTimezoneStr, TD_TIMEZONE_LEN, "%s (%s, %s%02d00)", buf, tzname[isdst_now], tz >= 0 ? "+" : "-", abs(tz));
  *outDaylight = isdst_now;

#else
  code = setenv("TZ", buf, 1);
  if (-1 == code) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    return terrno;
  }

  tzset();
  int32_t tz = (int32_t)((-timezone * MILLISECOND_PER_SECOND) / MILLISECOND_PER_HOUR);
  *tsTimezone = tz;
  tz += isdst_now;
  (void)snprintf(outTimezoneStr, TD_TIMEZONE_LEN, "%s (%s, %s%02d00)", buf, tzname[isdst_now], tz >= 0 ? "+" : "-", abs(tz));
  *outDaylight = isdst_now;

#endif

  return code;
}

int32_t taosGetSystemTimezone(char *outTimezoneStr, enum TdTimezone *tsTimezone) {
  int32_t code  = 0;
#ifdef WINDOWS
  char  value[100];
  char  keyPath[100];
  DWORD bufferSize = sizeof(value);
  LONG result = RegGetValue(HKEY_LOCAL_MACHINE, "SYSTEM\\CurrentControlSet\\Control\\TimeZoneInformation", "TimeZoneKeyName",
              RRF_RT_ANY, NULL, (PVOID)&value, &bufferSize);
  if (result != ERROR_SUCCESS) {
    return TAOS_SYSTEM_WINAPI_ERROR(result);
  }
  tstrncpy(outTimezoneStr, "not configured", TD_TIMEZONE_LEN);
  *tsTimezone = 0;
  if (bufferSize > 0) {
    for (size_t i = 0; i < 139; i++) {
      if (strcmp(win_tz[i][0], value) == 0) {
        tstrncpy(outTimezoneStr, win_tz[i][1], TD_TIMEZONE_LEN);
        bufferSize = sizeof(value);
        snprintf(keyPath, sizeof(keyPath), "SOFTWARE\\Microsoft\\Windows NT\\CurrentVersion\\Time Zones\\%s", value);
        result = RegGetValue(HKEY_LOCAL_MACHINE, keyPath, "Display", RRF_RT_ANY, NULL, (PVOID)&value, &bufferSize);
        if (result != ERROR_SUCCESS) {
          return TAOS_SYSTEM_WINAPI_ERROR(result);
        }
        if (bufferSize > 0) {
          // value[4] = (value[4] == '+' ? '-' : '+');
          snprintf(outTimezoneStr, TD_TIMEZONE_LEN, "%s (UTC, %c%c%c%c%c)", outTimezoneStr, value[4], value[5], value[6], value[8],
                  value[9]);
          *tsTimezone = taosStr2Int32(&value[4], NULL, 10);
        }
        break;
      }
    }
  }
  return 0;
#elif defined(_TD_DARWIN_64)
  char  buf[4096] = {0};
  char *tz = NULL;
  {
    int n = readlink("/etc/localtime", buf, sizeof(buf));
    if (n < 0) {
      return TSDB_CODE_TIME_ERROR;
    }
    buf[n] = '\0';

    char *zi = strstr(buf, "zoneinfo");
    if (!zi) {
      return TSDB_CODE_TIME_ERROR;
    }
    tz = zi + strlen("zoneinfo") + 1;

    code = setenv("TZ", tz, 1);
    if (-1 == code) {
      terrno = TAOS_SYSTEM_ERROR(errno);
      return terrno;
    }
    tzset();
  }

  /*
   * NOTE: do not remove it.
   * Enforce set the correct daylight saving time(DST) flag according
   * to current time
   */
  time_t    tx1 = taosGetTimestampSec();
  struct tm tm1;
  if (taosLocalTime(&tx1, &tm1, NULL, 0) == NULL) {
    return TSDB_CODE_TIME_ERROR;
  }
  daylight = tm1.tm_isdst;
  isdst_now = tm1.tm_isdst;

  /*
   * format example:
   *
   * Asia/Shanghai   (CST, +0800)
   * Europe/London   (BST, +0100)
   */
  snprintf(outTimezoneStr, TD_TIMEZONE_LEN, "%s (%s, %+03ld00)", tz, tm1.tm_isdst ? tzname[daylight] : tzname[0],
           -timezone / 3600);
  return 0;
#else

  char  buf[4096] = {0};
  char *tz = NULL;
  {
    int n = readlink("/etc/localtime", buf, sizeof(buf)-1);
    if (n < 0) {
      if (taosCheckExistFile("/etc/timezone")) {
        /*
         * NOTE: do not remove it.
         * Enforce set the correct daylight saving time(DST) flag according
         * to current time
         */
        time_t    tx1 = taosGetTimestampSec();
        struct tm tm1;
        if(taosLocalTime(&tx1, &tm1, NULL, 0) == NULL) {
          return TSDB_CODE_TIME_ERROR;
        }
        /* load time zone string from /etc/timezone */
        // FILE *f = fopen("/etc/timezone", "r");
        errno = 0;
        TdFilePtr pFile = taosOpenFile("/etc/timezone", TD_FILE_READ);
        char      buf[68] = {0};
        if (pFile != NULL) {
          int len = taosReadFile(pFile, buf, 64);
          if (len < 0) {
            TAOS_UNUSED(taosCloseFile(&pFile));
            return TSDB_CODE_TIME_ERROR;
          }

          TAOS_UNUSED(taosCloseFile(&pFile));

          buf[sizeof(buf) - 1] = 0;
          char *lineEnd = strstr(buf, "\n");
          if (lineEnd != NULL) {
            *lineEnd = 0;
          }

          // for CentOS system, /etc/timezone does not exist. Ignore the TZ environment variables
          if (strlen(buf) > 0) {
            code = setenv("TZ", buf, 1);
            if (-1 == code) {
              terrno = TAOS_SYSTEM_ERROR(errno);
              return terrno;
            }
          }
        }
        // get and set default timezone
        tzset();
        /*
         * get CURRENT time zone.
         * system current time zone is affected by daylight saving time(DST)
         *
         * e.g., the local time zone of London in DST is GMT+01:00,
         * otherwise is GMT+00:00
         */
        int32_t tz = (-timezone * MILLISECOND_PER_SECOND) / MILLISECOND_PER_HOUR;
        *tsTimezone = tz;
        tz += daylight;

        /*
         * format example:
         *
         * Asia/Shanghai   (CST, +0800)
         * Europe/London   (BST, +0100)
         */
        (void)snprintf(outTimezoneStr, TD_TIMEZONE_LEN, "%s (%s, %s%02d00)", buf, tzname[daylight], tz >= 0 ? "+" : "-",
                 abs(tz));
      } else {
        return TSDB_CODE_TIME_ERROR;
      }
      return  0;
    }
    buf[n] = '\0';

    char *zi = strstr(buf, "zoneinfo");
    if (!zi) {
      return TSDB_CODE_TIME_ERROR;
    }
    tz = zi + strlen("zoneinfo") + 1;

    code = setenv("TZ", tz, 1);
    if (-1 == code) {
      terrno = TAOS_SYSTEM_ERROR(errno);
      return terrno;
    }
    tzset();
  }

  /*
   * NOTE: do not remove it.
   * Enforce set the correct daylight saving time(DST) flag according
   * to current time
   */
  time_t    tx1 = taosGetTimestampSec();
  struct tm tm1;
  if(taosLocalTime(&tx1, &tm1, NULL, 0) == NULL) {
    return TSDB_CODE_TIME_ERROR;
  }
  isdst_now = tm1.tm_isdst;

  /*
   * format example:
   *
   * Asia/Shanghai   (CST, +0800)
   * Europe/London   (BST, +0100)
   */
  (void)snprintf(outTimezoneStr, TD_TIMEZONE_LEN, "%s (%s, %+03ld00)", tz, tm1.tm_isdst ? tzname[daylight] : tzname[0],
           -timezone / 3600);
  return 0;
#endif
}

#if defined THREAD_SAFE && THREAD_SAFE
# include <pthread.h>
static pthread_mutex_t locallock = PTHREAD_MUTEX_INITIALIZER;
static int lock(void) { return pthread_mutex_lock(&locallock); }
static void unlock(void) { pthread_mutex_unlock(&locallock); }
#else
static int lock(void) { return 0; }
static void unlock(void) { }
#endif

#ifndef TZ_ABBR_CHAR_SET
# define TZ_ABBR_CHAR_SET \
	"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789 :+-._"
#endif /* !defined TZ_ABBR_CHAR_SET */

#ifndef TZ_ABBR_ERR_CHAR
# define TZ_ABBR_ERR_CHAR '_'
#endif /* !defined TZ_ABBR_ERR_CHAR */

/*
** Support non-POSIX platforms that distinguish between text and binary files.
*/

#ifndef O_BINARY
# define O_BINARY 0
#endif

#ifndef WILDABBR
/*
** Someone might make incorrect use of a time zone abbreviation:
**	1.	They might reference tzname[0] before calling tzset (explicitly
**		or implicitly).
**	2.	They might reference tzname[1] before calling tzset (explicitly
**		or implicitly).
**	3.	They might reference tzname[1] after setting to a time zone
**		in which Daylight Saving Time is never observed.
**	4.	They might reference tzname[0] after setting to a time zone
**		in which Standard Time is never observed.
**	5.	They might reference tm.TM_ZONE after calling offtime.
** What's best to do in the above cases is open to debate;
** for now, we just set things up so that in any of the five cases
** WILDABBR is used. Another possibility: initialize tzname[0] to the
** string "tzname[0] used before set", and similarly for the other cases.
** And another: initialize tzname[0] to "ERA", with an explanation in the
** manual page of what this "time zone abbreviation" means (doing this so
** that tzname[0] has the "normal" length of three characters).
*/
# define WILDABBR "   "
#endif /* !defined WILDABBR */

static const char	wildabbr[] = WILDABBR;

static char const etc_utc[] = "Etc/UTC";
static char const *utc = etc_utc + sizeof "Etc/" - 1;

/*
** The DST rules to use if TZ has no rules and we can't load TZDEFRULES.
** Default to US rules as of 2017-05-07.
** POSIX does not specify the default DST rules;
** for historical reasons, US rules are a common default.
*/
#ifndef TZDEFRULESTRING
# define TZDEFRULESTRING ",M3.2.0,M11.1.0"
#endif

struct ttinfo {				/* time type information */
  int_fast32_t	tt_utoff;	/* UT offset in seconds */
  bool		tt_isdst;	/* used to set tm_isdst */
  int		tt_desigidx;	/* abbreviation list index */
  bool		tt_ttisstd;	/* transition is std time */
  bool		tt_ttisut;	/* transition is UT */
};

struct lsinfo {				/* leap second information */
  time_t		ls_trans;	/* transition time */
  int_fast32_t	ls_corr;	/* correction to apply */
};

/* This abbreviation means local time is unspecified.  */
static char const UNSPEC[] = "-00";

/* How many extra bytes are needed at the end of struct state's chars array.
   This needs to be at least 1 for null termination in case the input
   data isn't properly terminated, and it also needs to be big enough
   for ttunspecified to work without crashing.  */
enum { CHARS_EXTRA = max_tz(sizeof UNSPEC, 2) - 1 };

/* Limit to time zone abbreviation length in proleptic TZ strings.
   This is distinct from TZ_MAX_CHARS, which limits TZif file contents.  */
#ifndef TZNAME_MAXIMUM
# define TZNAME_MAXIMUM 255
#endif

/* A representation of the contents of a TZif file.  Ideally this
   would have no size limits; the following sizes should suffice for
   practical use.  This struct should not be too large, as instances
   are put on the stack and stacks are relatively small on some platforms.
   See tzfile.h for more about the sizes.  */
struct state {
  int		leapcnt;
  int		timecnt;
  int		typecnt;
  int		charcnt;
  bool		goback;
  bool		goahead;
  time_t		ats[TZ_MAX_TIMES];
  unsigned char	types[TZ_MAX_TIMES];
  struct ttinfo	ttis[TZ_MAX_TYPES];
  char chars[max_tz(max_tz(TZ_MAX_CHARS + CHARS_EXTRA, sizeof "UTC"),
                 2 * (TZNAME_MAXIMUM + 1))];
  struct lsinfo	lsis[TZ_MAX_LEAPS];
};

enum r_type {
  JULIAN_DAY,		/* Jn = Julian day */
  DAY_OF_YEAR,		/* n = day of year */
  MONTH_NTH_DAY_OF_WEEK	/* Mm.n.d = month, week, day of week */
};

struct rule {
  enum r_type	r_type;		/* type of rule */
  int		r_day;		/* day number of rule */
  int		r_week;		/* week number of rule */
  int		r_mon;		/* month number of rule */
  int_fast32_t	r_time;		/* transition time of rule */
};

static struct tm *gmtsub(struct state const *, time_t const *, int_fast32_t,
                         struct tm *);
static bool increment_overflow(int *, int);
static bool increment_overflow_time(time_t *, int_fast32_t);
static int_fast32_t leapcorr(struct state const *, time_t);
static bool normalize_overflow32(int_fast32_t *, int *, int);
static struct tm *timesub(time_t const *, int_fast32_t, struct state const *,
                          struct tm *);
static bool tzparse(char const *, struct state *, struct state const *);

#ifdef ALL_STATE
static struct state *	lclptr;
static struct state *	gmtptr;
#endif /* defined ALL_STATE */

#ifndef ALL_STATE
static struct state	lclmem;
static struct state	gmtmem;
static struct state *const lclptr = &lclmem;
static struct state *const gmtptr = &gmtmem;
#endif /* State Farm */

#ifndef TZ_STRLEN_MAX
# define TZ_STRLEN_MAX 255
#endif /* !defined TZ_STRLEN_MAX */

static char		lcl_TZname[TZ_STRLEN_MAX + 1];
static int		lcl_is_set;

/*
** Section 4.12.3 of X3.159-1989 requires that
**	Except for the strftime function, these functions [asctime,
**	ctime, gmtime, localtime] return values in one of two static
**	objects: a broken-down time structure and an array of char.
** Thanks to Paul Eggert for noting this.
**
** Although this requirement was removed in C99 it is still present in POSIX.
** Follow the requirement if SUPPORT_C89, even though this is more likely to
** trigger latent bugs in programs.
*/

#if SUPPORT_C89
static struct tm	tm;
#endif

#if 2 <= HAVE_TZNAME + TZ_TIME_T
char *			tzname[2] = {
	(char *) wildabbr,
	(char *) wildabbr
};
#endif
#if 2 <= USG_COMPAT + TZ_TIME_T
long			timezone;
int			daylight;
#endif
#if 2 <= ALTZONE + TZ_TIME_T
long			altzone;
#endif

/* Initialize *S to a value based on UTOFF, ISDST, and DESIGIDX.  */
static void
init_ttinfo(struct ttinfo *s, int_fast32_t utoff, bool isdst, int desigidx)
{
  s->tt_utoff = utoff;
  s->tt_isdst = isdst;
  s->tt_desigidx = desigidx;
  s->tt_ttisstd = false;
  s->tt_ttisut = false;
}

/* Return true if SP's time type I does not specify local time.  */
static bool
ttunspecified(struct state const *sp, int i)
{
  char const *abbr = &sp->chars[sp->ttis[i].tt_desigidx];
  /* memcmp is likely faster than strcmp, and is safe due to CHARS_EXTRA.  */
  return memcmp(abbr, UNSPEC, sizeof UNSPEC) == 0;
}

static int_fast32_t
detzcode(const char *const codep)
{
  register int_fast32_t	result;
  register int		i;
  int_fast32_t one = 1;
  int_fast32_t halfmaxval = one << (32 - 2);
  int_fast32_t maxval = halfmaxval - 1 + halfmaxval;
  int_fast32_t minval = -1 - maxval;

  result = codep[0] & 0x7f;
  for (i = 1; i < 4; ++i)
    result = (result << 8) | (codep[i] & 0xff);

  if (codep[0] & 0x80) {
    /* Do two's-complement negation even on non-two's-complement machines.
       If the result would be minval - 1, return minval.  */
    result -= !TWOS_COMPLEMENT(int_fast32_t) && result != 0;
    result += minval;
  }
  return result;
}

static int_fast64_t
detzcode64(const char *const codep)
{
  register int_fast64_t result;
  register int	i;
  int_fast64_t one = 1;
  int_fast64_t halfmaxval = one << (64 - 2);
  int_fast64_t maxval = halfmaxval - 1 + halfmaxval;
  int_fast64_t minval = -TWOS_COMPLEMENT(int_fast64_t) - maxval;

  result = codep[0] & 0x7f;
  for (i = 1; i < 8; ++i)
    result = (result << 8) | (codep[i] & 0xff);

  if (codep[0] & 0x80) {
    /* Do two's-complement negation even on non-two's-complement machines.
       If the result would be minval - 1, return minval.  */
    result -= !TWOS_COMPLEMENT(int_fast64_t) && result != 0;
    result += minval;
  }
  return result;
}

static void
update_tzname_etc(struct state const *sp, struct ttinfo const *ttisp)
{
#if HAVE_TZNAME
  tzname[ttisp->tt_isdst] = (char *) &sp->chars[ttisp->tt_desigidx];
#endif
#if USG_COMPAT
  if (!ttisp->tt_isdst)
    timezone = - ttisp->tt_utoff;
#endif
#if ALTZONE
  if (ttisp->tt_isdst)
    altzone = - ttisp->tt_utoff;
#endif
}

/* If STDDST_MASK indicates that SP's TYPE provides useful info,
   update tzname, timezone, and/or altzone and return STDDST_MASK,
   diminished by the provided info if it is a specified local time.
   Otherwise, return STDDST_MASK.  See settzname for STDDST_MASK.  */
static int
may_update_tzname_etc(int stddst_mask, struct state *sp, int type)
{
  struct ttinfo *ttisp = &sp->ttis[type];
  int this_bit = 1 << ttisp->tt_isdst;
  if (stddst_mask & this_bit) {
    update_tzname_etc(sp, ttisp);
    if (!ttunspecified(sp, type))
      return stddst_mask & ~this_bit;
  }
  return stddst_mask;
}

static void
settzname(void)
{
  register struct state * const	sp = lclptr;
  register int			i;

  /* If STDDST_MASK & 1 we need info about a standard time.
     If STDDST_MASK & 2 we need info about a daylight saving time.
     When STDDST_MASK becomes zero we can stop looking.  */
  int stddst_mask = 0;

#if HAVE_TZNAME
  tzname[0] = tzname[1] = (char *) (sp ? wildabbr : utc);
	stddst_mask = 3;
#endif
#if USG_COMPAT
  timezone = 0;
	stddst_mask = 3;
#endif
#if ALTZONE
  altzone = 0;
	stddst_mask |= 2;
#endif
  /*
  ** And to get the latest time zone abbreviations into tzname. . .
  */
  if (sp) {
    for (i = sp->timecnt - 1; stddst_mask && 0 <= i; i--)
      stddst_mask = may_update_tzname_etc(stddst_mask, sp, sp->types[i]);
    for (i = sp->typecnt - 1; stddst_mask && 0 <= i; i--)
      stddst_mask = may_update_tzname_etc(stddst_mask, sp, i);
  }
#if USG_COMPAT
  daylight = stddst_mask >> 1 ^ 1;
#endif
}

/* Replace bogus characters in time zone abbreviations.
   Return 0 on success, an errno value if a time zone abbreviation is
   too long.  */
static int
scrub_abbrs(struct state *sp)
{
  int i;

  /* Reject overlong abbreviations.  */
  for (i = 0; i < sp->charcnt - (TZNAME_MAXIMUM + 1); ) {
    int len = strlen(&sp->chars[i]);
    if (TZNAME_MAXIMUM < len)
      return EOVERFLOW;
    i += len + 1;
  }

  /* Replace bogus characters.  */
  for (i = 0; i < sp->charcnt; ++i)
    if (strchr(TZ_ABBR_CHAR_SET, sp->chars[i]) == NULL)
      sp->chars[i] = TZ_ABBR_ERR_CHAR;

  return 0;
}

/* Input buffer for data read from a compiled tz file.  */
union input_buffer {
  /* The first part of the buffer, interpreted as a header.  */
  struct tzhead tzhead;

  /* The entire buffer.  Ideally this would have no size limits;
     the following should suffice for practical use.  */
  char buf[2 * sizeof(struct tzhead) + 2 * sizeof(struct state)
           + 4 * TZ_MAX_TIMES];
};

/* TZDIR with a trailing '/' rather than a trailing '\0'.  */
static char const tzdirslash[sizeof TZDIR] = TZDIR "/";

/* Local storage needed for 'tzloadbody'.  */
union local_storage {
  /* The results of analyzing the file's contents after it is opened.  */
  struct file_analysis {
    /* The input buffer.  */
    union input_buffer u;

    /* A temporary state used for parsing a TZ string in the file.  */
    struct state st;
  } u;

  /* The name of the file to be opened.  Ideally this would have no
     size limits, to support arbitrarily long Zone names.
     Limiting Zone names to 1024 bytes should suffice for practical use.
     However, there is no need for this to be smaller than struct
     file_analysis as that struct is allocated anyway, as the other
     union member.  */
  char fullname[max_tz(sizeof(struct file_analysis), sizeof tzdirslash + 1024)];
};

/* Load tz data from the file named NAME into *SP.  Read extended
   format if DOEXTEND.  Use *LSP for temporary storage.  Return 0 on
   success, an errno value on failure.  */
static int
tzloadbody(char const *name, struct state *sp, bool doextend,
           union local_storage *lsp)
{
  register int			i;
  register int			fid;
  register int			stored;
  register ssize_t		nread;
  register bool doaccess;
  register union input_buffer *up = &lsp->u.u;
  register int tzheadsize = sizeof(struct tzhead);

  sp->goback = sp->goahead = false;

  if (! name) {
    name = TZDEFAULT;
    if (! name)
      return EINVAL;
  }

  if (name[0] == ':')
    ++name;
#ifdef SUPPRESS_TZDIR
  /* Do not prepend TZDIR.  This is intended for specialized
	   applications only, due to its security implications.  */
	doaccess = true;
#else
  doaccess = name[0] == '/';
#endif
  if (!doaccess) {
    char const *dot;
    if (sizeof lsp->fullname - sizeof tzdirslash <= strlen(name))
      return ENAMETOOLONG;

    /* Create a string "TZDIR/NAME".  Using sprintf here
       would pull in stdio (and would fail if the
       resulting string length exceeded INT_MAX!).  */
    memcpy(lsp->fullname, tzdirslash, sizeof tzdirslash);
    strcpy(lsp->fullname + sizeof tzdirslash, name);

    /* Set doaccess if NAME contains a ".." file name
       component, as such a name could read a file outside
       the TZDIR virtual subtree.  */
    for (dot = name; (dot = strchr(dot, '.')); dot++)
      if ((dot == name || dot[-1] == '/') && dot[1] == '.'
          && (dot[2] == '/' || !dot[2])) {
        doaccess = true;
        break;
      }

    name = lsp->fullname;
  }
  if (doaccess && access(name, R_OK) != 0)
    return errno;
  fid = open(name, O_RDONLY | O_BINARY);
  if (fid < 0)
    return errno;

  nread = read(fid, up->buf, sizeof up->buf);
  if (nread < tzheadsize) {
    int err = nread < 0 ? errno : EINVAL;
    close(fid);
    return err;
  }
  if (close(fid) < 0)
    return errno;
  for (stored = 4; stored <= 8; stored *= 2) {
    char version = up->tzhead.tzh_version[0];
    bool skip_datablock = stored == 4 && version;
    int_fast32_t datablock_size;
    int_fast32_t ttisstdcnt = detzcode(up->tzhead.tzh_ttisstdcnt);
    int_fast32_t ttisutcnt = detzcode(up->tzhead.tzh_ttisutcnt);
    int_fast64_t prevtr = -1;
    int_fast32_t prevcorr;
    int_fast32_t leapcnt = detzcode(up->tzhead.tzh_leapcnt);
    int_fast32_t timecnt = detzcode(up->tzhead.tzh_timecnt);
    int_fast32_t typecnt = detzcode(up->tzhead.tzh_typecnt);
    int_fast32_t charcnt = detzcode(up->tzhead.tzh_charcnt);
    char const *p = up->buf + tzheadsize;
    /* Although tzfile(5) currently requires typecnt to be nonzero,
       support future formats that may allow zero typecnt
       in files that have a TZ string and no transitions.  */
    if (! (0 <= leapcnt && leapcnt < TZ_MAX_LEAPS
           && 0 <= typecnt && typecnt < TZ_MAX_TYPES
           && 0 <= timecnt && timecnt < TZ_MAX_TIMES
           && 0 <= charcnt && charcnt < TZ_MAX_CHARS
           && 0 <= ttisstdcnt && ttisstdcnt < TZ_MAX_TYPES
           && 0 <= ttisutcnt && ttisutcnt < TZ_MAX_TYPES))
      return EINVAL;
    datablock_size
        = (timecnt * stored		/* ats */
           + timecnt		/* types */
           + typecnt * 6		/* ttinfos */
           + charcnt		/* chars */
           + leapcnt * (stored + 4)	/* lsinfos */
           + ttisstdcnt		/* ttisstds */
           + ttisutcnt);		/* ttisuts */
    if (nread < tzheadsize + datablock_size)
      return EINVAL;
    if (skip_datablock)
      p += datablock_size;
    else {
      if (! ((ttisstdcnt == typecnt || ttisstdcnt == 0)
             && (ttisutcnt == typecnt || ttisutcnt == 0)))
        return EINVAL;

      sp->leapcnt = leapcnt;
      sp->timecnt = timecnt;
      sp->typecnt = typecnt;
      sp->charcnt = charcnt;

      /* Read transitions, discarding those out of time_t range.
         But pretend the last transition before TIME_T_MIN
         occurred at TIME_T_MIN.  */
      timecnt = 0;
      for (i = 0; i < sp->timecnt; ++i) {
        int_fast64_t at
            = stored == 4 ? detzcode(p) : detzcode64(p);
        sp->types[i] = at <= TIME_T_MAX;
        if (sp->types[i]) {
          time_t attime
              = ((TYPE_SIGNED(time_t) ? at < TIME_T_MIN : at < 0)
                 ? TIME_T_MIN : at);
          if (timecnt && attime <= sp->ats[timecnt - 1]) {
            if (attime < sp->ats[timecnt - 1])
              return EINVAL;
            sp->types[i - 1] = 0;
            timecnt--;
          }
          sp->ats[timecnt++] = attime;
        }
        p += stored;
      }

      timecnt = 0;
      for (i = 0; i < sp->timecnt; ++i) {
        unsigned char typ = *p++;
        if (sp->typecnt <= typ)
          return EINVAL;
        if (sp->types[i])
          sp->types[timecnt++] = typ;
      }
      sp->timecnt = timecnt;
      for (i = 0; i < sp->typecnt; ++i) {
        register struct ttinfo *	ttisp;
        unsigned char isdst, desigidx;

        ttisp = &sp->ttis[i];
        ttisp->tt_utoff = detzcode(p);
        p += 4;
        isdst = *p++;
        if (! (isdst < 2))
          return EINVAL;
        ttisp->tt_isdst = isdst;
        desigidx = *p++;
        if (! (desigidx < sp->charcnt))
          return EINVAL;
        ttisp->tt_desigidx = desigidx;
      }
      for (i = 0; i < sp->charcnt; ++i)
        sp->chars[i] = *p++;
      /* Ensure '\0'-terminated, and make it safe to call
         ttunspecified later.  */
      memset(&sp->chars[i], 0, CHARS_EXTRA);

      /* Read leap seconds, discarding those out of time_t range.  */
      leapcnt = 0;
      for (i = 0; i < sp->leapcnt; ++i) {
        int_fast64_t tr = stored == 4 ? detzcode(p) : detzcode64(p);
        int_fast32_t corr = detzcode(p + stored);
        p += stored + 4;

        /* Leap seconds cannot occur before the Epoch,
           or out of order.  */
        if (tr <= prevtr)
          return EINVAL;

        /* To avoid other botches in this code, each leap second's
           correction must differ from the previous one's by 1
           second or less, except that the first correction can be
           any value; these requirements are more generous than
           RFC 8536, to allow future RFC extensions.  */
        if (! (i == 0
               || (prevcorr < corr
                   ? corr == prevcorr + 1
                   : (corr == prevcorr
                      || corr == prevcorr - 1))))
          return EINVAL;
        prevtr = tr;
        prevcorr = corr;

        if (tr <= TIME_T_MAX) {
          sp->lsis[leapcnt].ls_trans = tr;
          sp->lsis[leapcnt].ls_corr = corr;
          leapcnt++;
        }
      }
      sp->leapcnt = leapcnt;

      for (i = 0; i < sp->typecnt; ++i) {
        register struct ttinfo *	ttisp;

        ttisp = &sp->ttis[i];
        if (ttisstdcnt == 0)
          ttisp->tt_ttisstd = false;
        else {
          if (*p != true && *p != false)
            return EINVAL;
          ttisp->tt_ttisstd = *p++;
        }
      }
      for (i = 0; i < sp->typecnt; ++i) {
        register struct ttinfo *	ttisp;

        ttisp = &sp->ttis[i];
        if (ttisutcnt == 0)
          ttisp->tt_ttisut = false;
        else {
          if (*p != true && *p != false)
            return EINVAL;
          ttisp->tt_ttisut = *p++;
        }
      }
    }

    nread -= p - up->buf;
    memmove(up->buf, p, nread);

    /* If this is an old file, we're done.  */
    if (!version)
      break;
  }
  if (doextend && nread > 2 &&
      up->buf[0] == '\n' && up->buf[nread - 1] == '\n' &&
      sp->typecnt + 2 <= TZ_MAX_TYPES) {
    struct state	*ts = &lsp->u.st;

    up->buf[nread - 1] = '\0';
    if (tzparse(&up->buf[1], ts, sp)) {

      /* Attempt to reuse existing abbreviations.
         Without this, America/Anchorage would be right on
         the edge after 2037 when TZ_MAX_CHARS is 50, as
         sp->charcnt equals 40 (for LMT AST AWT APT AHST
         AHDT YST AKDT AKST) and ts->charcnt equals 10
         (for AKST AKDT).  Reusing means sp->charcnt can
         stay 40 in this example.  */
      int gotabbr = 0;
      int charcnt = sp->charcnt;
      for (i = 0; i < ts->typecnt; i++) {
        char *tsabbr = ts->chars + ts->ttis[i].tt_desigidx;
        int j;
        for (j = 0; j < charcnt; j++)
          if (strcmp(sp->chars + j, tsabbr) == 0) {
            ts->ttis[i].tt_desigidx = j;
            gotabbr++;
            break;
          }
        if (! (j < charcnt)) {
          int tsabbrlen = strlen(tsabbr);
          if (j + tsabbrlen < TZ_MAX_CHARS) {
            strcpy(sp->chars + j, tsabbr);
            charcnt = j + tsabbrlen + 1;
            ts->ttis[i].tt_desigidx = j;
            gotabbr++;
          }
        }
      }
      if (gotabbr == ts->typecnt) {
        sp->charcnt = charcnt;

        /* Ignore any trailing, no-op transitions generated
           by zic as they don't help here and can run afoul
           of bugs in zic 2016j or earlier.  */
        while (1 < sp->timecnt
               && (sp->types[sp->timecnt - 1]
                   == sp->types[sp->timecnt - 2]))
          sp->timecnt--;

        sp->goahead = ts->goahead;

        for (i = 0; i < ts->timecnt; i++) {
          time_t t = ts->ats[i];
          if (increment_overflow_time(&t, leapcorr(sp, t))
              || (0 < sp->timecnt
                  && t <= sp->ats[sp->timecnt - 1]))
            continue;
          if (TZ_MAX_TIMES <= sp->timecnt) {
            sp->goahead = false;
            break;
          }
          sp->ats[sp->timecnt] = t;
          sp->types[sp->timecnt] = (sp->typecnt
                                    + ts->types[i]);
          sp->timecnt++;
        }
        for (i = 0; i < ts->typecnt; i++)
          sp->ttis[sp->typecnt++] = ts->ttis[i];
      }
    }
  }
  if (sp->typecnt == 0)
    return EINVAL;

  return 0;
}

/* Load tz data from the file named NAME into *SP.  Read extended
   format if DOEXTEND.  Return 0 on success, an errno value on failure.  */
static int
tzload(char const *name, struct state *sp, bool doextend)
{
#ifdef ALL_STATE
  union local_storage *lsp = malloc(sizeof *lsp);
  if (!lsp) {
    return HAVE_MALLOC_ERRNO ? errno : ENOMEM;
  } else {
    int err = tzloadbody(name, sp, doextend, lsp);
    free(lsp);
    return err;
  }
#else
  union local_storage ls;
  return tzloadbody(name, sp, doextend, &ls);
#endif
}

static const int	mon_lengths[2][MONSPERYEAR] = {
    { 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31 },
    { 31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31 }
};

static const int	year_lengths[2] = {
    DAYSPERNYEAR, DAYSPERLYEAR
};

/* Is C an ASCII digit?  */
static bool
is_digit(char c)
{
  return '0' <= c && c <= '9';
}

/*
** Given a pointer into a timezone string, scan until a character that is not
** a valid character in a time zone abbreviation is found.
** Return a pointer to that character.
*/

ATTRIBUTE_PURE_114833 static const char *
getzname(register const char *strp)
{
  register char	c;

  while ((c = *strp) != '\0' && !is_digit(c) && c != ',' && c != '-' &&
         c != '+')
    ++strp;
  return strp;
}

/*
** Given a pointer into an extended timezone string, scan until the ending
** delimiter of the time zone abbreviation is located.
** Return a pointer to the delimiter.
**
** As with getzname above, the legal character set is actually quite
** restricted, with other characters producing undefined results.
** We don't do any checking here; checking is done later in common-case code.
*/

ATTRIBUTE_PURE_114833 static const char *
getqzname(register const char *strp, const int delim)
{
  register int	c;

  while ((c = *strp) != '\0' && c != delim)
    ++strp;
  return strp;
}

/*
** Given a pointer into a timezone string, extract a number from that string.
** Check that the number is within a specified range; if it is not, return
** NULL.
** Otherwise, return a pointer to the first character not part of the number.
*/

static const char *
getnum(register const char *strp, int *const nump, const int min, const int max)
{
  register char	c;
  register int	num;

  if (strp == NULL || !is_digit(c = *strp))
    return NULL;
  num = 0;
  do {
    num = num * 10 + (c - '0');
    if (num > max)
      return NULL;	/* illegal value */
    c = *++strp;
  } while (is_digit(c));
  if (num < min)
    return NULL;		/* illegal value */
  *nump = num;
  return strp;
}

/*
** Given a pointer into a timezone string, extract a number of seconds,
** in hh[:mm[:ss]] form, from the string.
** If any error occurs, return NULL.
** Otherwise, return a pointer to the first character not part of the number
** of seconds.
*/

static const char *
getsecs(register const char *strp, int_fast32_t *const secsp)
{
  int	num;
  int_fast32_t secsperhour = SECSPERHOUR;

  /*
  ** 'HOURSPERDAY * DAYSPERWEEK - 1' allows quasi-POSIX rules like
  ** "M10.4.6/26", which does not conform to POSIX,
  ** but which specifies the equivalent of
  ** "02:00 on the first Sunday on or after 23 Oct".
  */
  strp = getnum(strp, &num, 0, HOURSPERDAY * DAYSPERWEEK - 1);
  if (strp == NULL)
    return NULL;
  *secsp = num * secsperhour;
  if (*strp == ':') {
    ++strp;
    strp = getnum(strp, &num, 0, MINSPERHOUR - 1);
    if (strp == NULL)
      return NULL;
    *secsp += num * SECSPERMIN;
    if (*strp == ':') {
      ++strp;
      /* 'SECSPERMIN' allows for leap seconds.  */
      strp = getnum(strp, &num, 0, SECSPERMIN);
      if (strp == NULL)
        return NULL;
      *secsp += num;
    }
  }
  return strp;
}

/*
** Given a pointer into a timezone string, extract an offset, in
** [+-]hh[:mm[:ss]] form, from the string.
** If any error occurs, return NULL.
** Otherwise, return a pointer to the first character not part of the time.
*/

static const char *
getoffset(register const char *strp, int_fast32_t *const offsetp)
{
  register bool neg = false;

  if (*strp == '-') {
    neg = true;
    ++strp;
  } else if (*strp == '+')
    ++strp;
  strp = getsecs(strp, offsetp);
  if (strp == NULL)
    return NULL;		/* illegal time */
  if (neg)
    *offsetp = -*offsetp;
  return strp;
}

/*
** Given a pointer into a timezone string, extract a rule in the form
** date[/time]. See POSIX Base Definitions section 8.3 variable TZ
** for the format of "date" and "time".
** If a valid rule is not found, return NULL.
** Otherwise, return a pointer to the first character not part of the rule.
*/

static const char *
getrule(const char *strp, register struct rule *const rulep)
{
  if (*strp == 'J') {
    /*
    ** Julian day.
    */
    rulep->r_type = JULIAN_DAY;
    ++strp;
    strp = getnum(strp, &rulep->r_day, 1, DAYSPERNYEAR);
  } else if (*strp == 'M') {
    /*
    ** Month, week, day.
    */
    rulep->r_type = MONTH_NTH_DAY_OF_WEEK;
    ++strp;
    strp = getnum(strp, &rulep->r_mon, 1, MONSPERYEAR);
    if (strp == NULL)
      return NULL;
    if (*strp++ != '.')
      return NULL;
    strp = getnum(strp, &rulep->r_week, 1, 5);
    if (strp == NULL)
      return NULL;
    if (*strp++ != '.')
      return NULL;
    strp = getnum(strp, &rulep->r_day, 0, DAYSPERWEEK - 1);
  } else if (is_digit(*strp)) {
    /*
    ** Day of year.
    */
    rulep->r_type = DAY_OF_YEAR;
    strp = getnum(strp, &rulep->r_day, 0, DAYSPERLYEAR - 1);
  } else	return NULL;		/* invalid format */
  if (strp == NULL)
    return NULL;
  if (*strp == '/') {
    /*
    ** Time specified.
    */
    ++strp;
    strp = getoffset(strp, &rulep->r_time);
  } else	rulep->r_time = 2 * SECSPERHOUR;	/* default = 2:00:00 */
  return strp;
}

/*
** Given a year, a rule, and the offset from UT at the time that rule takes
** effect, calculate the year-relative time that rule takes effect.
*/

static int_fast32_t
transtime(const int year, register const struct rule *const rulep,
          const int_fast32_t offset)
{
  register bool	leapyear;
  register int_fast32_t value;
  register int	i;
  int		d, m1, yy0, yy1, yy2, dow;

  leapyear = isleap(year);
  switch (rulep->r_type) {

    case JULIAN_DAY:
      /*
      ** Jn - Julian day, 1 == January 1, 60 == March 1 even in leap
      ** years.
      ** In non-leap years, or if the day number is 59 or less, just
      ** add SECSPERDAY times the day number-1 to the time of
      ** January 1, midnight, to get the day.
      */
      value = (rulep->r_day - 1) * SECSPERDAY;
      if (leapyear && rulep->r_day >= 60)
        value += SECSPERDAY;
      break;

    case DAY_OF_YEAR:
      /*
      ** n - day of year.
      ** Just add SECSPERDAY times the day number to the time of
      ** January 1, midnight, to get the day.
      */
      value = rulep->r_day * SECSPERDAY;
      break;

    case MONTH_NTH_DAY_OF_WEEK:
      /*
      ** Mm.n.d - nth "dth day" of month m.
      */

      /*
      ** Use Zeller's Congruence to get day-of-week of first day of
      ** month.
      */
      m1 = (rulep->r_mon + 9) % 12 + 1;
      yy0 = (rulep->r_mon <= 2) ? (year - 1) : year;
      yy1 = yy0 / 100;
      yy2 = yy0 % 100;
      dow = ((26 * m1 - 2) / 10 +
             1 + yy2 + yy2 / 4 + yy1 / 4 - 2 * yy1) % 7;
      if (dow < 0)
        dow += DAYSPERWEEK;

      /*
      ** "dow" is the day-of-week of the first day of the month. Get
      ** the day-of-month (zero-origin) of the first "dow" day of the
      ** month.
      */
      d = rulep->r_day - dow;
      if (d < 0)
        d += DAYSPERWEEK;
      for (i = 1; i < rulep->r_week; ++i) {
        if (d + DAYSPERWEEK >=
            mon_lengths[leapyear][rulep->r_mon - 1])
          break;
        d += DAYSPERWEEK;
      }

      /*
      ** "d" is the day-of-month (zero-origin) of the day we want.
      */
      value = d * SECSPERDAY;
      for (i = 0; i < rulep->r_mon - 1; ++i)
        value += mon_lengths[leapyear][i] * SECSPERDAY;
      break;

    default: unreachable();
  }

  /*
  ** "value" is the year-relative time of 00:00:00 UT on the day in
  ** question. To get the year-relative time of the specified local
  ** time on that day, add the transition time and the current offset
  ** from UT.
  */
  return value + rulep->r_time + offset;
}

/*
** Given a POSIX.1 proleptic TZ string, fill in the rule tables as
** appropriate.
*/

static bool
tzparse(const char *name, struct state *sp, struct state const *basep)
{
  const char *			stdname;
  const char *			dstname;
  int_fast32_t			stdoffset;
  int_fast32_t			dstoffset;
  register char *			cp;
  register bool			load_ok;
  ptrdiff_t stdlen, dstlen, charcnt;
  time_t atlo = TIME_T_MIN, leaplo = TIME_T_MIN;

  stdname = name;
  if (*name == '<') {
    name++;
    stdname = name;
    name = getqzname(name, '>');
    if (*name != '>')
      return false;
    stdlen = name - stdname;
    name++;
  } else {
    name = getzname(name);
    stdlen = name - stdname;
  }
  if (! (0 < stdlen && stdlen <= TZNAME_MAXIMUM))
    return false;
  name = getoffset(name, &stdoffset);
  if (name == NULL)
    return false;
  charcnt = stdlen + 1;
  if (basep) {
    if (0 < basep->timecnt)
      atlo = basep->ats[basep->timecnt - 1];
    load_ok = false;
    sp->leapcnt = basep->leapcnt;
    memcpy(sp->lsis, basep->lsis, sp->leapcnt * sizeof *sp->lsis);
  } else {
    load_ok = tzload(TZDEFRULES, sp, false) == 0;
    if (!load_ok)
      sp->leapcnt = 0;	/* So, we're off a little.  */
  }
  if (0 < sp->leapcnt)
    leaplo = sp->lsis[sp->leapcnt - 1].ls_trans;
  sp->goback = sp->goahead = false;
  if (*name != '\0') {
    if (*name == '<') {
      dstname = ++name;
      name = getqzname(name, '>');
      if (*name != '>')
        return false;
      dstlen = name - dstname;
      name++;
    } else {
      dstname = name;
      name = getzname(name);
      dstlen = name - dstname; /* length of DST abbr. */
    }
    if (! (0 < dstlen && dstlen <= TZNAME_MAXIMUM))
      return false;
    charcnt += dstlen + 1;
    if (*name != '\0' && *name != ',' && *name != ';') {
      name = getoffset(name, &dstoffset);
      if (name == NULL)
        return false;
    } else	dstoffset = stdoffset - SECSPERHOUR;
    if (*name == '\0' && !load_ok)
      name = TZDEFRULESTRING;
    if (*name == ',' || *name == ';') {
      struct rule	start;
      struct rule	end;
      register int	year;
      register int	timecnt;
      time_t		janfirst;
      int_fast32_t janoffset = 0;
      int yearbeg, yearlim;

      ++name;
      if ((name = getrule(name, &start)) == NULL)
        return false;
      if (*name++ != ',')
        return false;
      if ((name = getrule(name, &end)) == NULL)
        return false;
      if (*name != '\0')
        return false;
      sp->typecnt = 2;	/* standard time and DST */
      /*
      ** Two transitions per year, from EPOCH_YEAR forward.
      */
      init_ttinfo(&sp->ttis[0], -stdoffset, false, 0);
      init_ttinfo(&sp->ttis[1], -dstoffset, true, stdlen + 1);
      timecnt = 0;
      janfirst = 0;
      yearbeg = EPOCH_YEAR;

      do {
        int_fast32_t yearsecs
            = year_lengths[isleap(yearbeg - 1)] * SECSPERDAY;
        time_t janfirst1 = janfirst;
        yearbeg--;
        if (increment_overflow_time(&janfirst1, -yearsecs)) {
          janoffset = -yearsecs;
          break;
        }
        janfirst = janfirst1;
      } while (atlo < janfirst
               && EPOCH_YEAR - YEARSPERREPEAT / 2 < yearbeg);

      while (true) {
        int_fast32_t yearsecs
            = year_lengths[isleap(yearbeg)] * SECSPERDAY;
        int yearbeg1 = yearbeg;
        time_t janfirst1 = janfirst;
        if (increment_overflow_time(&janfirst1, yearsecs)
            || increment_overflow(&yearbeg1, 1)
            || atlo <= janfirst1)
          break;
        yearbeg = yearbeg1;
        janfirst = janfirst1;
      }

      yearlim = yearbeg;
      if (increment_overflow(&yearlim, years_of_observations))
        yearlim = INT_MAX;
      for (year = yearbeg; year < yearlim; year++) {
        int_fast32_t
            starttime = transtime(year, &start, stdoffset),
            endtime = transtime(year, &end, dstoffset);
        int_fast32_t
            yearsecs = (year_lengths[isleap(year)]
                        * SECSPERDAY);
        bool reversed = endtime < starttime;
        if (reversed) {
          int_fast32_t swap = starttime;
          starttime = endtime;
          endtime = swap;
        }
        if (reversed
            || (starttime < endtime
                && endtime - starttime < yearsecs)) {
          if (TZ_MAX_TIMES - 2 < timecnt)
            break;
          sp->ats[timecnt] = janfirst;
          if (! increment_overflow_time
              (&sp->ats[timecnt],
               janoffset + starttime)
              && atlo <= sp->ats[timecnt])
            sp->types[timecnt++] = !reversed;
          sp->ats[timecnt] = janfirst;
          if (! increment_overflow_time
              (&sp->ats[timecnt],
               janoffset + endtime)
              && atlo <= sp->ats[timecnt]) {
            sp->types[timecnt++] = reversed;
          }
        }
        if (endtime < leaplo) {
          yearlim = year;
          if (increment_overflow(&yearlim,
                                 years_of_observations))
            yearlim = INT_MAX;
        }
        if (increment_overflow_time
            (&janfirst, janoffset + yearsecs))
          break;
        janoffset = 0;
      }
      sp->timecnt = timecnt;
      if (! timecnt) {
        sp->ttis[0] = sp->ttis[1];
        sp->typecnt = 1;	/* Perpetual DST.  */
      } else if (years_of_observations <= year - yearbeg)
        sp->goback = sp->goahead = true;
    } else {
      register int_fast32_t	theirstdoffset;
      register int_fast32_t	theirdstoffset;
      register int_fast32_t	theiroffset;
      register bool		isdst;
      register int		i;
      register int		j;

      if (*name != '\0')
        return false;
      /*
      ** Initial values of theirstdoffset and theirdstoffset.
      */
      theirstdoffset = 0;
      for (i = 0; i < sp->timecnt; ++i) {
        j = sp->types[i];
        if (!sp->ttis[j].tt_isdst) {
          theirstdoffset =
              - sp->ttis[j].tt_utoff;
          break;
        }
      }
      theirdstoffset = 0;
      for (i = 0; i < sp->timecnt; ++i) {
        j = sp->types[i];
        if (sp->ttis[j].tt_isdst) {
          theirdstoffset =
              - sp->ttis[j].tt_utoff;
          break;
        }
      }
      /*
      ** Initially we're assumed to be in standard time.
      */
      isdst = false;
      /*
      ** Now juggle transition times and types
      ** tracking offsets as you do.
      */
      for (i = 0; i < sp->timecnt; ++i) {
        j = sp->types[i];
        sp->types[i] = sp->ttis[j].tt_isdst;
        if (sp->ttis[j].tt_ttisut) {
          /* No adjustment to transition time */
        } else {
          /*
          ** If daylight saving time is in
          ** effect, and the transition time was
          ** not specified as standard time, add
          ** the daylight saving time offset to
          ** the transition time; otherwise, add
          ** the standard time offset to the
          ** transition time.
          */
          /*
          ** Transitions from DST to DDST
          ** will effectively disappear since
          ** proleptic TZ strings have only one
          ** DST offset.
          */
          if (isdst && !sp->ttis[j].tt_ttisstd) {
            sp->ats[i] += dstoffset -
                          theirdstoffset;
          } else {
            sp->ats[i] += stdoffset -
                          theirstdoffset;
          }
        }
        theiroffset = -sp->ttis[j].tt_utoff;
        if (sp->ttis[j].tt_isdst)
          theirdstoffset = theiroffset;
        else	theirstdoffset = theiroffset;
      }
      /*
      ** Finally, fill in ttis.
      */
      init_ttinfo(&sp->ttis[0], -stdoffset, false, 0);
      init_ttinfo(&sp->ttis[1], -dstoffset, true, stdlen + 1);
      sp->typecnt = 2;
    }
  } else {
    dstlen = 0;
    sp->typecnt = 1;		/* only standard time */
    sp->timecnt = 0;
    init_ttinfo(&sp->ttis[0], -stdoffset, false, 0);
  }
  sp->charcnt = charcnt;
  cp = sp->chars;
  memcpy(cp, stdname, stdlen);
  cp += stdlen;
  *cp++ = '\0';
  if (dstlen != 0) {
    memcpy(cp, dstname, dstlen);
    *(cp + dstlen) = '\0';
  }
  return true;
}

static void
gmtload(struct state *const sp)
{
  if (tzload(etc_utc, sp, true) != 0)
    tzparse("UTC0", sp, NULL);
}

/* Initialize *SP to a value appropriate for the TZ setting NAME.
   Return 0 on success, an errno value on failure.  */
static int
zoneinit(struct state *sp, char const *name)
{
  if (name && ! name[0]) {
    /*
    ** User wants it fast rather than right.
    */
    sp->leapcnt = 0;		/* so, we're off a little */
    sp->timecnt = 0;
    sp->typecnt = 0;
    sp->charcnt = 0;
    sp->goback = sp->goahead = false;
    init_ttinfo(&sp->ttis[0], 0, false, 0);
    strcpy(sp->chars, utc);
    return 0;
  } else {
    int err = tzload(name, sp, true);
    if (err != 0 && name && name[0] != ':' && tzparse(name, sp, NULL))
      err = 0;
    if (err == 0)
      err = scrub_abbrs(sp);
    return err;
  }
}

static void
tzset_unlocked(void)
{
  char const *name = getenv("TZ");
  struct state *sp = lclptr;
  int lcl = name ? strlen(name) < sizeof lcl_TZname : -1;
  if (lcl < 0
      ? lcl_is_set < 0
      : 0 < lcl_is_set && strcmp(lcl_TZname, name) == 0)
    return;
#ifdef ALL_STATE
  if (! sp)
    lclptr = sp = malloc(sizeof *lclptr);
#endif /* defined ALL_STATE */
  if (sp) {
    if (zoneinit(sp, name) != 0)
      zoneinit(sp, "");
    if (0 < lcl)
      strcpy(lcl_TZname, name);
  }
  settzname();
  lcl_is_set = lcl;
}

void
tzset(void)
{
  if (lock() != 0)
    return;
  tzset_unlocked();
  unlock();
}

static void
gmtcheck(void)
{
  static bool gmt_is_set;
  if (lock() != 0)
    return;
  if (! gmt_is_set) {
#ifdef ALL_STATE
    gmtptr = malloc(sizeof *gmtptr);
#endif
    if (gmtptr)
      gmtload(gmtptr);
    gmt_is_set = true;
  }
  unlock();
}

#if NETBSD_INSPIRED

timezone_t
tzalloc(char const *name)
{
  timezone_t sp = malloc(sizeof *sp);
  if (sp) {
    int err = zoneinit(sp, name);
    if (err != 0) {
      free(sp);
      errno = err;
      return NULL;
    }
  } else if (!HAVE_MALLOC_ERRNO)
    errno = ENOMEM;
  return sp;
}

void
tzfree(timezone_t sp)
{
  free(sp);
}

/*
** NetBSD 6.1.4 has ctime_rz, but omit it because C23 deprecates ctime and
** POSIX.1-2024 removes ctime_r.  Both have potential security problems that
** ctime_rz would share.  Callers can instead use localtime_rz + strftime.
**
** NetBSD 6.1.4 has tzgetname, but omit it because it doesn't work
** in zones with three or more time zone abbreviations.
** Callers can instead use localtime_rz + strftime.
*/

#endif

/*
** The easy way to behave "as if no library function calls" localtime
** is to not call it, so we drop its guts into "localsub", which can be
** freely called. (And no, the PANS doesn't require the above behavior,
** but it *is* desirable.)
**
** If successful and SETNAME is nonzero,
** set the applicable parts of tzname, timezone and altzone;
** however, it's OK to omit this step for proleptic TZ strings
** since in that case tzset should have already done this step correctly.
** SETNAME's type is int_fast32_t for compatibility with gmtsub,
** but it is actually a boolean and its value should be 0 or 1.
*/

/*ARGSUSED*/
static struct tm *
localsub(struct state const *sp, time_t const *timep, int_fast32_t setname,
         struct tm *const tmp)
{
  register const struct ttinfo *	ttisp;
  register int			i;
  register struct tm *		result;
  const time_t			t = *timep;

  if (sp == NULL) {
    /* Don't bother to set tzname etc.; tzset has already done it.  */
    return gmtsub(gmtptr, timep, 0, tmp);
  }
  if ((sp->goback && t < sp->ats[0]) ||
      (sp->goahead && t > sp->ats[sp->timecnt - 1])) {
    time_t newt;
    register time_t		seconds;
    register time_t		years;

    if (t < sp->ats[0])
      seconds = sp->ats[0] - t;
    else	seconds = t - sp->ats[sp->timecnt - 1];
    --seconds;

    /* Beware integer overflow, as SECONDS might
       be close to the maximum time_t.  */
    years = seconds / SECSPERREPEAT * YEARSPERREPEAT;
    seconds = years * AVGSECSPERYEAR;
    years += YEARSPERREPEAT;
    if (t < sp->ats[0])
      newt = t + seconds + SECSPERREPEAT;
    else
      newt = t - seconds - SECSPERREPEAT;

    if (newt < sp->ats[0] ||
        newt > sp->ats[sp->timecnt - 1])
      return NULL;	/* "cannot happen" */
    result = localsub(sp, &newt, setname, tmp);
    if (result) {
#if defined ckd_add && defined ckd_sub
      if (t < sp->ats[0]
				    ? ckd_sub(&result->tm_year,
					      result->tm_year, years)
				    : ckd_add(&result->tm_year,
					      result->tm_year, years))
				  return NULL;
#else
      register int_fast64_t newy;

      newy = result->tm_year;
      if (t < sp->ats[0])
        newy -= years;
      else	newy += years;
      if (! (INT_MIN <= newy && newy <= INT_MAX))
        return NULL;
      result->tm_year = newy;
#endif
    }
    return result;
  }
  if (sp->timecnt == 0 || t < sp->ats[0]) {
    i = 0;
  } else {
    register int	lo = 1;
    register int	hi = sp->timecnt;

    while (lo < hi) {
      register int	mid = (lo + hi) >> 1;

      if (t < sp->ats[mid])
        hi = mid;
      else	lo = mid + 1;
    }
    i = sp->types[lo - 1];
  }
  ttisp = &sp->ttis[i];
  /*
  ** To get (wrong) behavior that's compatible with System V Release 2.0
  ** you'd replace the statement below with
  **	t += ttisp->tt_utoff;
  **	timesub(&t, 0L, sp, tmp);
  */
  result = timesub(&t, ttisp->tt_utoff, sp, tmp);
  if (result) {
    result->tm_isdst = ttisp->tt_isdst;
#ifdef TM_ZONE
    result->TM_ZONE = (char *) &sp->chars[ttisp->tt_desigidx];
#endif /* defined TM_ZONE */
    if (setname)
      update_tzname_etc(sp, ttisp);
  }
  return result;
}

#if NETBSD_INSPIRED

struct tm *
localtime_rz(struct state *sp, time_t const *timep,
	     struct tm *tmp)
{
  return localsub(sp, timep, 0, tmp);
}

#endif

static struct tm *
localtime_tzset(time_t const *timep, struct tm *tmp, bool setname)
{
  int err = lock();
  if (err) {
    errno = err;
    return NULL;
  }
  if (setname || !lcl_is_set)
    tzset_unlocked();
  tmp = localsub(lclptr, timep, setname, tmp);
  unlock();
  return tmp;
}

struct tm *
localtime(const time_t *timep)
{
#if !SUPPORT_C89
  static struct tm tm;
#endif
  return localtime_tzset(timep, &tm, true);
}

struct tm *
localtime_r(const time_t *restrict timep, struct tm *restrict tmp)
{
  return localtime_tzset(timep, tmp, false);
}

/*
** gmtsub is to gmtime as localsub is to localtime.
*/

static struct tm *
gmtsub(ATTRIBUTE_MAYBE_UNUSED struct state const *sp, time_t const *timep,
       int_fast32_t offset, struct tm *tmp)
{
  register struct tm *	result;

  result = timesub(timep, offset, gmtptr, tmp);
#ifdef TM_ZONE
  /*
	** Could get fancy here and deliver something such as
	** "+xx" or "-xx" if offset is non-zero,
	** but this is no time for a treasure hunt.
	*/
	tmp->TM_ZONE = ((char *)
			(offset ? wildabbr : gmtptr ? gmtptr->chars : utc));
#endif /* defined TM_ZONE */
  return result;
}

/*
* Re-entrant version of gmtime.
*/

struct tm *
gmtime_r(time_t const *restrict timep, struct tm *restrict tmp)
{
  gmtcheck();
  return gmtsub(gmtptr, timep, 0, tmp);
}

struct tm *
gmtime(const time_t *timep)
{
#if !SUPPORT_C89
  static struct tm tm;
#endif
  return gmtime_r(timep, &tm);
}

#if STD_INSPIRED

/* This function is obsolescent and may disappear in future releases.
   Callers can instead use localtime_rz with a fixed-offset zone.  */

struct tm *
offtime(const time_t *timep, long offset)
{
  gmtcheck();

#if !SUPPORT_C89
  static struct tm tm;
#endif
  return gmtsub(gmtptr, timep, offset, &tm);
}

#endif

/*
** Return the number of leap years through the end of the given year
** where, to make the math easy, the answer for year zero is defined as zero.
*/

static time_t
leaps_thru_end_of_nonneg(time_t y)
{
  return y / 4 - y / 100 + y / 400;
}

static time_t
leaps_thru_end_of(time_t y)
{
  return (y < 0
          ? -1 - leaps_thru_end_of_nonneg(-1 - y)
          : leaps_thru_end_of_nonneg(y));
}

static struct tm *
timesub(const time_t *timep, int_fast32_t offset,
        const struct state *sp, struct tm *tmp)
{
  register const struct lsinfo *	lp;
  register time_t			tdays;
  register const int *		ip;
  register int_fast32_t		corr;
  register int			i;
  int_fast32_t idays, rem, dayoff, dayrem;
  time_t y;

  /* If less than SECSPERMIN, the number of seconds since the
     most recent positive leap second; otherwise, do not add 1
     to localtime tm_sec because of leap seconds.  */
  time_t secs_since_posleap = SECSPERMIN;

  corr = 0;
  i = (sp == NULL) ? 0 : sp->leapcnt;
  while (--i >= 0) {
    lp = &sp->lsis[i];
    if (*timep >= lp->ls_trans) {
      corr = lp->ls_corr;
      if ((i == 0 ? 0 : lp[-1].ls_corr) < corr)
        secs_since_posleap = *timep - lp->ls_trans;
      break;
    }
  }

  /* Calculate the year, avoiding integer overflow even if
     time_t is unsigned.  */
  tdays = *timep / SECSPERDAY;
  rem = *timep % SECSPERDAY;
  rem += offset % SECSPERDAY - corr % SECSPERDAY + 3 * SECSPERDAY;
  dayoff = offset / SECSPERDAY - corr / SECSPERDAY + rem / SECSPERDAY - 3;
  rem %= SECSPERDAY;
  /* y = (EPOCH_YEAR
          + floor((tdays + dayoff) / DAYSPERREPEAT) * YEARSPERREPEAT),
     sans overflow.  But calculate against 1570 (EPOCH_YEAR -
     YEARSPERREPEAT) instead of against 1970 so that things work
     for localtime values before 1970 when time_t is unsigned.  */
  dayrem = tdays % DAYSPERREPEAT;
  dayrem += dayoff % DAYSPERREPEAT;
  y = (EPOCH_YEAR - YEARSPERREPEAT
       + ((1 + dayoff / DAYSPERREPEAT + dayrem / DAYSPERREPEAT
           - ((dayrem % DAYSPERREPEAT) < 0)
           + tdays / DAYSPERREPEAT)
          * YEARSPERREPEAT));
  /* idays = (tdays + dayoff) mod DAYSPERREPEAT, sans overflow.  */
  idays = tdays % DAYSPERREPEAT;
  idays += dayoff % DAYSPERREPEAT + 2 * DAYSPERREPEAT;
  idays %= DAYSPERREPEAT;
  /* Increase Y and decrease IDAYS until IDAYS is in range for Y.  */
  while (year_lengths[isleap(y)] <= idays) {
    int tdelta = idays / DAYSPERLYEAR;
    int_fast32_t ydelta = tdelta + !tdelta;
    time_t newy = y + ydelta;
    register int	leapdays;
    leapdays = leaps_thru_end_of(newy - 1) -
               leaps_thru_end_of(y - 1);
    idays -= ydelta * DAYSPERNYEAR;
    idays -= leapdays;
    y = newy;
  }

#ifdef ckd_add
  if (ckd_add(&tmp->tm_year, y, -TM_YEAR_BASE)) {
	  errno = EOVERFLOW;
	  return NULL;
	}
#else
  if (!TYPE_SIGNED(time_t) && y < TM_YEAR_BASE) {
    int signed_y = y;
    tmp->tm_year = signed_y - TM_YEAR_BASE;
  } else if ((!TYPE_SIGNED(time_t) || INT_MIN + TM_YEAR_BASE <= y)
             && y - TM_YEAR_BASE <= INT_MAX)
    tmp->tm_year = y - TM_YEAR_BASE;
  else {
    errno = EOVERFLOW;
    return NULL;
  }
#endif
  tmp->tm_yday = idays;
  /*
  ** The "extra" mods below avoid overflow problems.
  */
  tmp->tm_wday = (TM_WDAY_BASE
                  + ((tmp->tm_year % DAYSPERWEEK)
                     * (DAYSPERNYEAR % DAYSPERWEEK))
                  + leaps_thru_end_of(y - 1)
                  - leaps_thru_end_of(TM_YEAR_BASE - 1)
                  + idays);
  tmp->tm_wday %= DAYSPERWEEK;
  if (tmp->tm_wday < 0)
    tmp->tm_wday += DAYSPERWEEK;
  tmp->tm_hour = rem / SECSPERHOUR;
  rem %= SECSPERHOUR;
  tmp->tm_min = rem / SECSPERMIN;
  tmp->tm_sec = rem % SECSPERMIN;

  /* Use "... ??:??:60" at the end of the localtime minute containing
     the second just before the positive leap second.  */
  tmp->tm_sec += secs_since_posleap <= tmp->tm_sec;

  ip = mon_lengths[isleap(y)];
  for (tmp->tm_mon = 0; idays >= ip[tmp->tm_mon]; ++(tmp->tm_mon))
    idays -= ip[tmp->tm_mon];
  tmp->tm_mday = idays + 1;
  tmp->tm_isdst = 0;
#ifdef TM_GMTOFF
  tmp->TM_GMTOFF = offset;
#endif /* defined TM_GMTOFF */
  return tmp;
}

/*
** Adapted from code provided by Robert Elz, who writes:
**	The "best" way to do mktime I think is based on an idea of Bob
**	Kridle's (so its said...) from a long time ago.
**	It does a binary search of the time_t space. Since time_t's are
**	just 32 bits, its a max of 32 iterations (even at 64 bits it
**	would still be very reasonable).
*/

#ifndef WRONG
# define WRONG (-1)
#endif /* !defined WRONG */

/*
** Normalize logic courtesy Paul Eggert.
*/

static bool
increment_overflow(int *ip, int j)
{
#ifdef ckd_add
  return ckd_add(ip, *ip, j);
#else
  register int const	i = *ip;

  /*
  ** If i >= 0 there can only be overflow if i + j > INT_MAX
  ** or if j > INT_MAX - i; given i >= 0, INT_MAX - i cannot overflow.
  ** If i < 0 there can only be overflow if i + j < INT_MIN
  ** or if j < INT_MIN - i; given i < 0, INT_MIN - i cannot overflow.
  */
  if ((i >= 0) ? (j > INT_MAX - i) : (j < INT_MIN - i))
    return true;
  *ip += j;
  return false;
#endif
}

static bool
increment_overflow32(int_fast32_t *const lp, int const m)
{
#ifdef ckd_add
  return ckd_add(lp, *lp, m);
#else
  register int_fast32_t const	l = *lp;

  if ((l >= 0) ? (m > INT_FAST32_MAX - l) : (m < INT_FAST32_MIN - l))
    return true;
  *lp += m;
  return false;
#endif
}

static bool
increment_overflow_time(time_t *tp, int_fast32_t j)
{
#ifdef ckd_add
  return ckd_add(tp, *tp, j);
#else
  /*
  ** This is like
  ** 'if (! (TIME_T_MIN <= *tp + j && *tp + j <= TIME_T_MAX)) ...',
  ** except that it does the right thing even if *tp + j would overflow.
  */
  if (! (j < 0
         ? (TYPE_SIGNED(time_t) ? TIME_T_MIN - j <= *tp : -1 - j < *tp)
         : *tp <= TIME_T_MAX - j))
    return true;
  *tp += j;
  return false;
#endif
}

static bool
normalize_overflow(int *const tensptr, int *const unitsptr, const int base)
{
  register int	tensdelta;

  tensdelta = (*unitsptr >= 0) ?
              (*unitsptr / base) :
              (-1 - (-1 - *unitsptr) / base);
  *unitsptr -= tensdelta * base;
  return increment_overflow(tensptr, tensdelta);
}

static bool
normalize_overflow32(int_fast32_t *tensptr, int *unitsptr, int base)
{
  register int	tensdelta;

  tensdelta = (*unitsptr >= 0) ?
              (*unitsptr / base) :
              (-1 - (-1 - *unitsptr) / base);
  *unitsptr -= tensdelta * base;
  return increment_overflow32(tensptr, tensdelta);
}

static int
tmcomp(register const struct tm *const atmp,
       register const struct tm *const btmp)
{
  register int	result;

  if (atmp->tm_year != btmp->tm_year)
    return atmp->tm_year < btmp->tm_year ? -1 : 1;
  if ((result = (atmp->tm_mon - btmp->tm_mon)) == 0 &&
      (result = (atmp->tm_mday - btmp->tm_mday)) == 0 &&
      (result = (atmp->tm_hour - btmp->tm_hour)) == 0 &&
      (result = (atmp->tm_min - btmp->tm_min)) == 0)
    result = atmp->tm_sec - btmp->tm_sec;
  return result;
}

/* Copy to *DEST from *SRC.  Copy only the members needed for mktime,
   as other members might not be initialized.  */
static void
mktmcpy(struct tm *dest, struct tm const *src)
{
  dest->tm_sec = src->tm_sec;
  dest->tm_min = src->tm_min;
  dest->tm_hour = src->tm_hour;
  dest->tm_mday = src->tm_mday;
  dest->tm_mon = src->tm_mon;
  dest->tm_year = src->tm_year;
  dest->tm_isdst = src->tm_isdst;
#if defined TM_GMTOFF && ! UNINIT_TRAP
  dest->TM_GMTOFF = src->TM_GMTOFF;
#endif
}

static time_t
time2sub(struct tm *const tmp,
         struct tm *(*funcp)(struct state const *, time_t const *,
                             int_fast32_t, struct tm *),
         struct state const *sp,
         const int_fast32_t offset,
         bool *okayp,
         bool do_norm_secs)
{
  register int			dir;
  register int			i, j;
  register int			saved_seconds;
  register int_fast32_t		li;
  register time_t			lo;
  register time_t			hi;
  int_fast32_t			y;
  time_t				newt;
  time_t				t;
  struct tm			yourtm, mytm;

  *okayp = false;
  mktmcpy(&yourtm, tmp);

  if (do_norm_secs) {
    if (normalize_overflow(&yourtm.tm_min, &yourtm.tm_sec,
                           SECSPERMIN))
      return WRONG;
  }
  if (normalize_overflow(&yourtm.tm_hour, &yourtm.tm_min, MINSPERHOUR))
    return WRONG;
  if (normalize_overflow(&yourtm.tm_mday, &yourtm.tm_hour, HOURSPERDAY))
    return WRONG;
  y = yourtm.tm_year;
  if (normalize_overflow32(&y, &yourtm.tm_mon, MONSPERYEAR))
    return WRONG;
  /*
  ** Turn y into an actual year number for now.
  ** It is converted back to an offset from TM_YEAR_BASE later.
  */
  if (increment_overflow32(&y, TM_YEAR_BASE))
    return WRONG;
  while (yourtm.tm_mday <= 0) {
    if (increment_overflow32(&y, -1))
      return WRONG;
    li = y + (1 < yourtm.tm_mon);
    yourtm.tm_mday += year_lengths[isleap(li)];
  }
  while (yourtm.tm_mday > DAYSPERLYEAR) {
    li = y + (1 < yourtm.tm_mon);
    yourtm.tm_mday -= year_lengths[isleap(li)];
    if (increment_overflow32(&y, 1))
      return WRONG;
  }
  for ( ; ; ) {
    i = mon_lengths[isleap(y)][yourtm.tm_mon];
    if (yourtm.tm_mday <= i)
      break;
    yourtm.tm_mday -= i;
    if (++yourtm.tm_mon >= MONSPERYEAR) {
      yourtm.tm_mon = 0;
      if (increment_overflow32(&y, 1))
        return WRONG;
    }
  }
#ifdef ckd_add
  if (ckd_add(&yourtm.tm_year, y, -TM_YEAR_BASE))
	  return WRONG;
#else
  if (increment_overflow32(&y, -TM_YEAR_BASE))
    return WRONG;
  if (! (INT_MIN <= y && y <= INT_MAX))
    return WRONG;
  yourtm.tm_year = y;
#endif
  if (yourtm.tm_sec >= 0 && yourtm.tm_sec < SECSPERMIN)
    saved_seconds = 0;
  else if (yourtm.tm_year < EPOCH_YEAR - TM_YEAR_BASE) {
    /*
    ** We can't set tm_sec to 0, because that might push the
    ** time below the minimum representable time.
    ** Set tm_sec to 59 instead.
    ** This assumes that the minimum representable time is
    ** not in the same minute that a leap second was deleted from,
    ** which is a safer assumption than using 58 would be.
    */
    if (increment_overflow(&yourtm.tm_sec, 1 - SECSPERMIN))
      return WRONG;
    saved_seconds = yourtm.tm_sec;
    yourtm.tm_sec = SECSPERMIN - 1;
  } else {
    saved_seconds = yourtm.tm_sec;
    yourtm.tm_sec = 0;
  }
  /*
  ** Do a binary search (this works whatever time_t's type is).
  */
  lo = TIME_T_MIN;
  hi = TIME_T_MAX;
  for ( ; ; ) {
    t = lo / 2 + hi / 2;
    if (t < lo)
      t = lo;
    else if (t > hi)
      t = hi;
    if (! funcp(sp, &t, offset, &mytm)) {
      /*
      ** Assume that t is too extreme to be represented in
      ** a struct tm; arrange things so that it is less
      ** extreme on the next pass.
      */
      dir = (t > 0) ? 1 : -1;
    } else	dir = tmcomp(&mytm, &yourtm);
    if (dir != 0) {
      if (t == lo) {
        if (t == TIME_T_MAX)
          return WRONG;
        ++t;
        ++lo;
      } else if (t == hi) {
        if (t == TIME_T_MIN)
          return WRONG;
        --t;
        --hi;
      }
      if (lo > hi)
        return WRONG;
      if (dir > 0)
        hi = t;
      else	lo = t;
      continue;
    }
#if defined TM_GMTOFF && ! UNINIT_TRAP
    if (mytm.TM_GMTOFF != yourtm.TM_GMTOFF
		    && (yourtm.TM_GMTOFF < 0
			? (-SECSPERDAY <= yourtm.TM_GMTOFF
			   && (mytm.TM_GMTOFF <=
			       (min_tz(INT_FAST32_MAX, LONG_MAX)
				+ yourtm.TM_GMTOFF)))
			: (yourtm.TM_GMTOFF <= SECSPERDAY
			   && ((max_tz(INT_FAST32_MIN, LONG_MIN)
				+ yourtm.TM_GMTOFF)
			       <= mytm.TM_GMTOFF)))) {
		  /* MYTM matches YOURTM except with the wrong UT offset.
		     YOURTM.TM_GMTOFF is plausible, so try it instead.
		     It's OK if YOURTM.TM_GMTOFF contains uninitialized data,
		     since the guess gets checked.  */
		  time_t altt = t;
		  int_fast32_t diff = mytm.TM_GMTOFF - yourtm.TM_GMTOFF;
		  if (!increment_overflow_time(&altt, diff)) {
		    struct tm alttm;
		    if (funcp(sp, &altt, offset, &alttm)
			&& alttm.tm_isdst == mytm.tm_isdst
			&& alttm.TM_GMTOFF == yourtm.TM_GMTOFF
			&& tmcomp(&alttm, &yourtm) == 0) {
		      t = altt;
		      mytm = alttm;
		    }
		  }
		}
#endif
    if (yourtm.tm_isdst < 0 || mytm.tm_isdst == yourtm.tm_isdst)
      break;
    /*
    ** Right time, wrong type.
    ** Hunt for right time, right type.
    ** It's okay to guess wrong since the guess
    ** gets checked.
    */
    if (sp == NULL)
      return WRONG;
    for (i = sp->typecnt - 1; i >= 0; --i) {
      if (sp->ttis[i].tt_isdst != yourtm.tm_isdst)
        continue;
      for (j = sp->typecnt - 1; j >= 0; --j) {
        if (sp->ttis[j].tt_isdst == yourtm.tm_isdst)
          continue;
        if (ttunspecified(sp, j))
          continue;
        newt = (t + sp->ttis[j].tt_utoff
                - sp->ttis[i].tt_utoff);
        if (! funcp(sp, &newt, offset, &mytm))
          continue;
        if (tmcomp(&mytm, &yourtm) != 0)
          continue;
        if (mytm.tm_isdst != yourtm.tm_isdst)
          continue;
        /*
        ** We have a match.
        */
        t = newt;
        goto label;
      }
    }
    return WRONG;
  }
  label:
  newt = t + saved_seconds;
  if ((newt < t) != (saved_seconds < 0))
    return WRONG;
  t = newt;
  if (funcp(sp, &t, offset, tmp))
    *okayp = true;
  return t;
}

static time_t
time2(struct tm * const	tmp,
      struct tm *(*funcp)(struct state const *, time_t const *,
                          int_fast32_t, struct tm *),
      struct state const *sp,
      const int_fast32_t offset,
      bool *okayp)
{
  time_t	t;

  /*
  ** First try without normalization of seconds
  ** (in case tm_sec contains a value associated with a leap second).
  ** If that fails, try with normalization of seconds.
  */
  t = time2sub(tmp, funcp, sp, offset, okayp, false);
  return *okayp ? t : time2sub(tmp, funcp, sp, offset, okayp, true);
}

static time_t
time1(struct tm *const tmp,
      struct tm *(*funcp)(struct state const *, time_t const *,
                          int_fast32_t, struct tm *),
      struct state const *sp,
      const int_fast32_t offset)
{
  register time_t			t;
  register int			samei, otheri;
  register int			sameind, otherind;
  register int			i;
  register int			nseen;
  char				seen[TZ_MAX_TYPES];
  unsigned char			types[TZ_MAX_TYPES];
  bool				okay;

  if (tmp == NULL) {
    errno = EINVAL;
    return WRONG;
  }
  if (tmp->tm_isdst > 1)
    tmp->tm_isdst = 1;
  t = time2(tmp, funcp, sp, offset, &okay);
  if (okay)
    return t;
  if (tmp->tm_isdst < 0)
#ifdef PCTS
    /*
		** POSIX Conformance Test Suite code courtesy Grant Sullivan.
		*/
		tmp->tm_isdst = 0;	/* reset to std and try again */
#else
    return t;
#endif /* !defined PCTS */
  /*
  ** We're supposed to assume that somebody took a time of one type
  ** and did some math on it that yielded a "struct tm" that's bad.
  ** We try to divine the type they started from and adjust to the
  ** type they need.
  */
  if (sp == NULL)
    return WRONG;
  for (i = 0; i < sp->typecnt; ++i)
    seen[i] = false;
  nseen = 0;
  for (i = sp->timecnt - 1; i >= 0; --i)
    if (!seen[sp->types[i]] && !ttunspecified(sp, sp->types[i])) {
      seen[sp->types[i]] = true;
      types[nseen++] = sp->types[i];
    }
  for (sameind = 0; sameind < nseen; ++sameind) {
    samei = types[sameind];
    if (sp->ttis[samei].tt_isdst != tmp->tm_isdst)
      continue;
    for (otherind = 0; otherind < nseen; ++otherind) {
      otheri = types[otherind];
      if (sp->ttis[otheri].tt_isdst == tmp->tm_isdst)
        continue;
      tmp->tm_sec += (sp->ttis[otheri].tt_utoff
                      - sp->ttis[samei].tt_utoff);
      tmp->tm_isdst = !tmp->tm_isdst;
      t = time2(tmp, funcp, sp, offset, &okay);
      if (okay)
        return t;
      tmp->tm_sec -= (sp->ttis[otheri].tt_utoff
                      - sp->ttis[samei].tt_utoff);
      tmp->tm_isdst = !tmp->tm_isdst;
    }
  }
  return WRONG;
}

static time_t
mktime_tzname(struct state *sp, struct tm *tmp, bool setname)
{
  if (sp)
    return time1(tmp, localsub, sp, setname);
  else {
    gmtcheck();
    return time1(tmp, gmtsub, gmtptr, 0);
  }
}

#if NETBSD_INSPIRED

time_t
mktime_z(struct state *sp, struct tm *tmp)
{
  return mktime_tzname(sp, tmp, false);
}

#endif

time_t
mktime(struct tm *tmp)
{
  time_t t;
  int err = lock();
  if (err) {
    errno = err;
    return -1;
  }
  tzset_unlocked();
  t = mktime_tzname(lclptr, tmp, true);
  unlock();
  return t;
}

#if STD_INSPIRED
/* This function is obsolescent and may disappear in future releases.
   Callers can instead use mktime.  */
time_t
timelocal(struct tm *tmp)
{
	if (tmp != NULL)
		tmp->tm_isdst = -1;	/* in case it wasn't initialized */
	return mktime(tmp);
}
#endif

#ifndef EXTERN_TIMEOFF
# ifndef timeoff
#  define timeoff my_timeoff /* Don't collide with OpenBSD 7.4 <time.h>.  */
# endif
# define EXTERN_TIMEOFF static
#endif

/* This function is obsolescent and may disappear in future releases.
   Callers can instead use mktime_z with a fixed-offset zone.  */
EXTERN_TIMEOFF time_t
timeoff(struct tm *tmp, long offset)
{
  if (tmp)
    tmp->tm_isdst = 0;
  gmtcheck();
  return time1(tmp, gmtsub, gmtptr, offset);
}

time_t
timegm(struct tm *tmp)
{
  time_t t;
  struct tm tmcpy;
  mktmcpy(&tmcpy, tmp);
  tmcpy.tm_wday = -1;
  t = timeoff(&tmcpy, 0);
  if (0 <= tmcpy.tm_wday)
    *tmp = tmcpy;
  return t;
}

static int_fast32_t
leapcorr(struct state const *sp, time_t t)
{
  register struct lsinfo const *	lp;
  register int			i;

  i = sp->leapcnt;
  while (--i >= 0) {
    lp = &sp->lsis[i];
    if (t >= lp->ls_trans)
      return lp->ls_corr;
  }
  return 0;
}
