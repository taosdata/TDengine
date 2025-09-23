"""
Build-in Tag system
"""


class CaseTag:
    """
    named tag
    """

    @classmethod
    def __tag_name__(cls):
        return cls.__qualname__[2:]


class T(CaseTag):
    class Cloud(CaseTag):
        class Connector(CaseTag):
            class Java(CaseTag):
                pass

            class Python(CaseTag):
                pass

            class Go(CaseTag):
                pass

            class Node(CaseTag):
                pass

            class Rust(CaseTag):
                pass

        class ThirdParty(CaseTag):
            pass

        class Toos(CaseTag):
            class TDengineCLI(CaseTag):
                pass

            class TaosDump(CaseTag):
                pass

            class TaosBenchmark(CaseTag):
                pass

    class Management(CaseTag):
        class Account:
            pass

        class Cluster(CaseTag):
            pass

        class InformationSchema(CaseTag):
            pass

        class PerformanceSchema(CaseTag):
            pass

    class Write(CaseTag):
        class Database(CaseTag):
            class Create(CaseTag):
                pass

            class Drop(CaseTag):
                pass

            class Alter(CaseTag):
                pass

        class Stable(CaseTag):
            class Create(CaseTag):
                pass

            class Drop(CaseTag):
                pass

            class Alter(CaseTag):
                pass

        class Table(CaseTag):
            class Create(CaseTag):
                pass

            class Drop(CaseTag):
                pass

            class Alter(CaseTag):
                pass

        class Stream(CaseTag):
            class Create(CaseTag):
                pass

            class Drop(CaseTag):
                pass

            class Alter(CaseTag):
                pass

        class TaosSql(CaseTag):
            class Update(CaseTag):
                pass

        class RestfulSql(CaseTag):
            class Database(CaseTag):
                class Create(CaseTag):
                    pass

                class Drop(CaseTag):
                    pass

                class Alter(CaseTag):
                    pass

            class Stable(CaseTag):
                class Create(CaseTag):
                    pass

                class Drop(CaseTag):
                    pass

                class Alter(CaseTag):
                    pass

            class Table(CaseTag):
                class Create(CaseTag):
                    class AutoCreate(CaseTag):
                        pass

                class Drop(CaseTag):
                    pass

                class Alter(CaseTag):
                    pass

            class Insert(CaseTag):
                class BatchInsert(CaseTag):
                    pass

                class SpecifiedColumn(CaseTag):
                    pass

                class Disorder(CaseTag):
                    pass

                class JsonTag(CaseTag):
                    pass

                class MultiTableInsert(CaseTag):
                    pass

                class TimestampTest(CaseTag):
                    pass

                class NullInsert(CaseTag):
                    pass

                class BoundaryTest(CaseTag):
                    class Tinyint(CaseTag):
                        pass

                    class Smallint(CaseTag):
                        pass

                    class Int(CaseTag):
                        pass

                    class Bigint(CaseTag):
                        pass

                    class Utinyint(CaseTag):
                        pass

                    class Usmallint(CaseTag):
                        pass

                    class Uint(CaseTag):
                        pass

                    class Ubigint(CaseTag):
                        pass

                    class Float(CaseTag):
                        pass

                    class Double(CaseTag):
                        pass

                    class Binary(CaseTag):
                        pass

                    class Nchar(CaseTag):
                        pass

                    class Bool(CaseTag):
                        pass

                    class Tag(CaseTag):
                        pass

                    class Column(CaseTag):
                        pass

            class Update(CaseTag):
                pass

            class Abnormal(CaseTag):
                pass

            class MultiThread(CaseTag):
                pass

        class TaoscSql(CaseTag):
            class Database(CaseTag):
                class Create(CaseTag):
                    pass

                class Drop(CaseTag):
                    pass

                class Alter(CaseTag):
                    pass

            class Stable(CaseTag):
                class Create(CaseTag):
                    pass

                class Drop(CaseTag):
                    pass

                class Alter(CaseTag):
                    pass

            class Table(CaseTag):
                class Create(CaseTag):
                    class AutoCreate(CaseTag):
                        pass

                class Drop(CaseTag):
                    pass

                class Alter(CaseTag):
                    pass

            class Insert(CaseTag):
                class BatchInsert(CaseTag):
                    pass

                class SpecifiedColumn(CaseTag):
                    pass

                class Disorder(CaseTag):
                    pass

                class JsonTag(CaseTag):
                    pass

                class MultiTableInsert(CaseTag):
                    pass

                class TimestampTest(CaseTag):
                    pass

                class NullInsert(CaseTag):
                    pass

                class BoundaryTest(CaseTag):
                    class Tinyint(CaseTag):
                        pass

                    class Smallint(CaseTag):
                        pass

                    class Int(CaseTag):
                        pass

                    class Bigint(CaseTag):
                        pass

                    class Utinyint(CaseTag):
                        pass

                    class Usmallint(CaseTag):
                        pass

                    class Uint(CaseTag):
                        pass

                    class Ubigint(CaseTag):
                        pass

                    class Float(CaseTag):
                        pass

                    class Double(CaseTag):
                        pass

                    class Binary(CaseTag):
                        pass

                    class Nchar(CaseTag):
                        pass

                    class Bool(CaseTag):
                        pass

                    class Tag(CaseTag):
                        pass

                    class Column(CaseTag):
                        pass

            class Update(CaseTag):
                pass

            class Abnormal(CaseTag):
                pass

            class MultiThread(CaseTag):
                pass

        class Schemaless(CaseTag):
            class Taosc(CaseTag):
                class InfluxDB(CaseTag):
                    pass

                class OpenTsDBTelnet(CaseTag):
                    pass

                class OpenTsDBJson(CaseTag):
                    pass

                class DB(CaseTag):
                    pass

            class Restful(CaseTag):
                class InfluxDB(CaseTag):
                    pass

                class OpenTsDBTelnet(CaseTag):
                    pass

                class OpenTsDBTelnetTCP(CaseTag):
                    pass

                class OpenTsDBJson(CaseTag):
                    pass

                class DB(CaseTag):
                    pass

    class Query(CaseTag):
        class TimelineDepend(CaseTag):
            class CSUM(CaseTag):
                pass

            class DERIVATIVE(CaseTag):
                pass

            class DIFF(CaseTag):
                pass

            class ELAPSED(CaseTag):
                pass

            class INTERP(CaseTag):
                pass

            class IRATE(CaseTag):
                pass

            class LEASTSQUARES(CaseTag):
                pass

            class MAVG(CaseTag):
                pass

            class SAMPLE(CaseTag):
                pass

            class TAIL(CaseTag):
                pass

            class TWA(CaseTag):
                pass

            class UNIQUE(CaseTag):
                pass

        class Math(CaseTag):
            class ABS(CaseTag):
                pass

            class ACOS(CaseTag):
                pass

            class ASIN(CaseTag):
                pass

            class ATAN(CaseTag):
                pass

            class CEIL(CaseTag):
                pass

            class COS(CaseTag):
                pass

            class FLOOR(CaseTag):
                pass

            class LOG(CaseTag):
                pass

            class POW(CaseTag):
                pass

            class ROUND(CaseTag):
                pass

            class SIN(CaseTag):
                pass

            class SQRT(CaseTag):
                pass

            class TAN(CaseTag):
                pass

        class StrFun(CaseTag):
            class CHAR_LENGTH(CaseTag):
                pass

            class CONCAT(CaseTag):
                pass

            class CONCAT_WS(CaseTag):
                pass

            class LENGTH(CaseTag):
                pass

        class ConvertFun(CaseTag):
            class CAST(CaseTag):
                pass

            class TO_ISO8601(CaseTag):
                pass

            class TO_JSON(CaseTag):
                pass

            class TO_UNIXTIMESTAMP(CaseTag):
                pass

    class Performance(CaseTag):
        class Benchmark(CaseTag):
            class Insert(CaseTag):
                class SingleMachine(CaseTag):
                    pass

                class Cluster(CaseTag):
                    pass

        class Query(CaseTag):
            pass

    class Abnormal(CaseTag):
        class Hardware(CaseTag):
            class Disk(CaseTag):
                pass

            class CPU(CaseTag):
                pass

            class Network(CaseTag):
                pass

            class Memory(CaseTag):
                pass

        class Software(CaseTag):
            class KillProcess(CaseTag):
                pass

            class FQDN(CaseTag):
                pass

            class File(CaseTag):
                pass

            class DropNode(CaseTag):
                pass

            class UpDownGrade(CaseTag):
                pass


if __name__ == '__main__':
    pass
