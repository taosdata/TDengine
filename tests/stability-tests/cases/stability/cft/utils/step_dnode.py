from .step_mnode import StepMnode

class StepDnode:
    """This class is used to operate the dnode in cluster
    """
    def __init__(self, tdSql, logger) -> None:
        # db connection instance
        self.con = tdSql
        # logger instance
        self.logger = logger
        self.mnode_utils = StepMnode(tdSql, logger)
    
    def get_dnodes(self, include_mnodes=True):
        """Get the dnode list from cluster
        :param include_mnodes: include mnodes or not
        :return: the dnode list which include id, endpoint
        """
        try:
            # client_0 = self.tdSql.get_connection(self._conf)
            sql = "show dnodes;"
            node_list = []
            self.con.query(sql)
            dnodes_data = self.con.query_data
            mnode_list = self.mnode_utils.get_mnodes()
            mnode_endpoint_list = [mnode[1] for mnode in mnode_list]
            i = 0
            while i < len(dnodes_data):
                if not include_mnodes:
                    if dnodes_data[i][1] in mnode_endpoint_list:
                        i = i + 1
                        continue
                # id, endpoint
                node_list.append((dnodes_data[i][0], dnodes_data[i][1]))
                i = i + 1
            return node_list
        except Exception as ex:
            raise Exception("Failed to get the dndoes with error: {}".format(str(ex)))
