import elasticsearch
import datetime
import elasticsearch.helpers
import json

class es():
    def __init__(self, es_config):
        _es_config = es_config

        try:
            if _es_config['use_ssl']:
                self.es_eng = elasticsearch.Elasticsearch(
                    _es_config['nodes'],
                    port=_es_config['port'],
                    http_auth=(_es_config['user'] + ':' + _es_config['password']),
                    verify_certs=_es_config['verify_certs'],
                    use_ssl=_es_config['use_ssl'],
                    ca_certs=_es_config['ca_cert']
                )
            else:
                self.es_eng = elasticsearch.Elasticsearch(
                    _es_config['es_nodes'],
                    port=_es_config['port'],
                    http_auth=(_es_config['user'] + ':' + _es_config['password'])
                )
        except Exception as _exc:
            print('ERR: [es:__init__]: Error with establishing connection with elastic cluster:', _exc)
            self.es_eng = False

    def bulk_insert(self, es_config, js_arr):
        _es_config = es_config
        _js_arr = js_arr
        _shards = _es_config['shards']
        _replicas = _es_config['replicas']
        _date_pattern = '{0:%Y}'.format(datetime.datetime.today())
        _index = _es_config['pattern'] + _date_pattern

        _map = {
            "mappings": {
                "properties": {
                    "timestamp": {"type": "date", "format": "yyyy-MM-dd' 'HH:mm"},
                    "showvvs_Name": {"type": "keyword"},
                    "storage_name": {"type": "keyword"},
                    "showvvd_Name": {"type": "keyword"},
                    "showvvd_VV_WWN": {"type": "keyword"},
                    "showvvs_Prov": {"type": "keyword"},
                    "showpd_Id": {"type": "keyword"},
                    "showpd_Node_WWN": {"type": "keyword"},
                    "showpd_State": {"type": "keyword"},
                    "showpd_Type": {"type": "keyword"},
                    "showld_Name": {"type": "keyword"},
                    "showld_State": {"type": "keyword"},
                    "showhost_Name": {"type": "keyword"},
                    "showhost_WWN": {"type": "keyword"},
                    "showpdvv_PDId": {"type": "keyword"},
                    "showpdvv_Type": {"type": "keyword"},
                    "showpdvv_VVId": {"type": "keyword"},
                    "showpdvv_VVName": {"type": "keyword"},
                    "showvlun_VVName": {"type": "keyword"},
                    "showvlun_HostName": {"type": "keyword"},
                    "showvlun_Host_WWN": {"type": "keyword"},
                    "showvlun_VV_WWN": {"type": "keyword"},
                    "showvlun_Status": {"type": "keyword"},
                    "statcpu_node": {"type": "keyword"},
                    "statcpu_cpu": {"type": "keyword"},
                    "statpd_ID": {"type": "keyword"},
                    "statvlun_VVname": {"type": "keyword"},
                    "statvlun_Host": {"type": "keyword"},
                    "statld_Ldname": {"type": "keyword"},
                    "cmd": {"type": "keyword"},
                    # "": {"type": "keyword"},
                    # "": {"type": "keyword"},
                    # "": {"type": "keyword"},
                    # "": {"type": "keyword"},
                    # "": {"type": "keyword"},
                    # "": {"type": "keyword"},
                    # "": {"type": "keyword"},
                    # "": {"type": "keyword"},
                    # "": {"type": "keyword"},
                    # "": {"type": "keyword"},
                    # "": {"type": "keyword"},
                    # "": {"type": "keyword"},
                    # "": {"type": "keyword"},
                    # "": {"type": "keyword"},
                    # "": {"type": "keyword"},
                    # "": {"type": "keyword"},
                    # "": {"type": "keyword"},
                    "showvvs_Adm_Rsvd": {"type": "float"},
                    "showvvs_Adm_Used": {"type": "float"},
                    "showvvs_Usr_pct_VSize_Used": {"type": "float"},
                    "showvvs_Snp_pct_VSize_Used": {"type": "float"},
                    "showpd_Size_MB": {"type": "float"},
                    "showpd_Free_MB": {"type": "float"},
                    "showpd_Volume_MB": {"type": "float"},
                    "showpd_Spare_MB": {"type": "float"},
                    "showpd_Failed_MB": {"type": "float"},
                    "statcpu_user": {"type": "float"},
                    "statcpu_sys": {"type": "float"},
                    "statcpu_idle": {"type": "float"},
                    "statcpu_intr": {"type": "float"},
                    "statcpu_ctxt": {"type": "float"},
                    "statpd_IO_per_second_Avg": {"type": "float"},
                    "statpd_KBytes_per_sec_Avg": {"type": "float"},
                    "statpd_Svt_ms_Avg": {"type": "float"},
                    "statpd_IOSz_KB_Avg": {"type": "float"},
                    "statpd_Idle_pct_Avg": {"type": "float"},
                    "statvlun_IO_per_second_Avg": {"type": "float"},
                    "statvlun_KBytes_per_sec_Avg": {"type": "float"},
                    "statvlun_Svt_ms_Avg": {"type": "float"},
                    "statvlun_IOSz_KB_Avg": {"type": "float"},
                    "statld_IO_per_second_Avg": {"type": "float"},
                    "statld_KBytes_per_sec_Avg": {"type": "float"},
                    "statld_Svt_ms_Avg": {"type": "float"},
                    "statld_IOSz_KB_Avg": {"type": "float"},
                    "showsys_Allocated": {"type": "long"},
                    "showsys_Allocated_System": {"type": "long"},
                    "showsys_Allocated_System_Internal": {"type": "long"},
                    "showsys_Allocated_System_Spare": {"type": "long"},
                    "showsys_Allocated_System_Spare_Unused": {"type": "long"},
                    "showsys_Allocated_System_Spare_Used": {"type": "long"},
                    "showsys_Allocated_Volumes": {"type": "long"},
                    "showsys_Allocated_Volumes_CPGs": {"type": "long"},
                    "showsys_Allocated_Volumes_CPGs_Admin": {"type": "long"},
                    "showsys_Allocated_Volumes_CPGs_Admin_Unused": {"type": "long"},
                    "showsys_Allocated_Volumes_CPGs_Admin_Used": {"type": "long"},
                    "showsys_Allocated_Volumes_CPGs_Admin_Used_Bulk": {"type": "long"},
                    "showsys_Allocated_Volumes_CPGs_Snapshot": {"type": "long"},
                    "showsys_Allocated_Volumes_CPGs_Snapshot_Unused": {"type": "long"},
                    "showsys_Allocated_Volumes_CPGs_Snapshot_Used": {"type": "long"},
                    "showsys_Allocated_Volumes_CPGs_Snapshot_Used_Bulk": {"type": "long"},
                    "showsys_Allocated_Volumes_CPGs_User": {"type": "long"},
                    "showsys_Allocated_Volumes_CPGs_User_Unused": {"type": "long"},
                    "showsys_Allocated_Volumes_CPGs_User_Used": {"type": "long"},
                    "showsys_Allocated_Volumes_CPGs_User_Used_Bulk": {"type": "long"},
                    "showsys_Allocated_Volumes_Non_CPGs": {"type": "long"},
                    "showsys_Allocated_Volumes_Non_CPGs_Admin": {"type": "long"},
                    "showsys_Allocated_Volumes_Non_CPGs_Snapshot": {"type": "long"},
                    "showsys_Allocated_Volumes_Non_CPGs_User": {"type": "long"},
                    "showsys_Allocated_Volumes_Unmapped": {"type": "long"},
                    "showsys_Failed": {"type": "long"},
                    "showsys_Free": {"type": "long"},
                    "showsys_Free_Initialized": {"type": "long"},
                    "showsys_Free_Uninitialized": {"type": "long"},
                    "showsys_Total_Capacity": {"type": "long"},
                    "showsys_Unavailable": {"type": "long"},
                    # "": {"type": "long"},
                    # "": {"type": "long"},
                    # "": {"type": "long"},
                    # "": {"type": "long"},
                    # "": {"type": "long"},
                    # "": {"type": "long"},
                    # "": {"type": "float"},
                    # "": {"type": "float"},
                    # "": {"type": "float"},
                    # "": {"type": "float"},
                    # "": {"type": "float"},
                    # "": {"type": "float"},
                    # "": {"type": "float"},
                    # "": {"type": "float"},
                    # "": {"type": "float"},
                    # "": {"type": "float"},
                    # "": {"type": "float"},
                    # "": {"type": "float"},
                    # "": {"type": "float"},
                    # "": {"type": "float"},
                    # "": {"type": "float"},
                    # "": {"type": "float"},
                    # "": {"type": "float"},
                    # "": {"type": "float"},
                    # "": {"type": "float"},
                    # "": {"type": "float"},
                    # "": {"type": "float"},
                    # "": {"type": "float"},
                    # "": {"type": "float"},
                    # "": {"type": "float"},
                    # "": {"type": "float"},
                    # "": {"type": "float"},
                    # "": {"type": "float"},
                    # "": {"type": "float"},
                    # "": {"type": "float"},
                    # "": {"type": "float"},
                }
            }
        }

        _body = {
            "settings": {
                "number_of_shards": _shards,
                "number_of_replicas": _replicas
            },
            "mappings": _map["mappings"]
        }
        _actions = [
            {
                "_index": _index,
                "_source": json.dumps(_js)
            }
            for _js in _js_arr
        ]

        if self.es_eng:
            if not self.es_eng.indices.exists(index=_index):
                try:
                    self.es_eng.indices.create(index=_index, body=_body)
                except Exception as _err:
                    print('ERR: [es:bulk_insert]', _err)
                    return False
            try:
                elasticsearch.helpers.bulk(self.es_eng, _actions, chunk_size=500, request_timeout=30)
            except Exception as _err:
                print('ERR: [es:bulk_insert]', _err)
                return False
            else:
                return True