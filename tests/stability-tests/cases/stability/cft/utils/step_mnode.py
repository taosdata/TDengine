

class StepMnode:
    """This class is used to operate the mnode in cluster
    """
    def __init__(self, tdSql, logger) -> None:
        # db connection instance
        self.con = tdSql
        # logger instance
        self.logger = logger

    def get_mnodes(self, role=None):
        """Get the mnode lsit from cluster
        :param role: the role of mnode, None as all, leader, follower, candidate
        :return: the mnode list which include mnode id, endpoint
        """
        try:
            sql = "show mnodes;"
            node_list = []
            self.con.query(sql)
            mnodes_data = self.con.query_data
            self.logger.info("Mnode info: {}".format(str(mnodes_data)))
            i = 0
            while i < len(mnodes_data):
                # check whether the mnode is offline
                if mnodes_data[i][2] == "offline":
                    raise Exception("Found offline mnode: {}".format(str(mnodes_data[i])))
                if role:
                    if mnodes_data[i][2] == role:
                        node_list.append((mnodes_data[i][0], mnodes_data[i][1]))
                else:
                    # get all mnodes id, endpoint
                    node_list.append((mnodes_data[i][0], mnodes_data[i][1]))
                i = i + 1
            return node_list
        except Exception as ex:
            raise Exception("Failed to get the mnode list with error: {}".format(str(ex)))

    def add_mnode(self, dnode_id):
        """Add the mnode in cluster
        :param dnode_id: the dnode is will be created as mnode
        """
        try:
            sql = "create mnode on dnode {};".format(str(dnode_id))
            self.con.execute(sql)
            self.logger.info("Create mnode on dnode {} successfully".format(str(dnode_id)))
        except Exception as ex:
            raise Exception("Failed to create the mnode {} with error: {}".format(str(dnode_id), str(ex)))

    def drop_mnode(self, dnode_id):
        """Delete the mnode in cluster
        :param dnode_id: the dnode is will be deleted
        """
        try:
            sql = "drop mnode on dnode {};".format(str(dnode_id))
            self.con.execute(sql)
            self.logger.info("Delete mnode on dnode {} successfully".format(str(dnode_id)))
        except Exception as ex:
            raise Exception("Failed to delete the mnode {} with error: {}".format(str(dnode_id), str(ex)))
