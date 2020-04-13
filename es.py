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
                    "showpdvv_PDId": {"type": "keyword"},
                    "showpdvv_VVName": {"type": "keyword"},
                    "showpdvv_VVId": {"type": "keyword"},
                    "statpd_ID": {"type": "keyword"},
                    "statvlun_Host": {"type": "keyword"},
                    "statvlun_VVname": {"type": "keyword"},
                    "statvlun_Lun": {"type": "keyword"},
                    "showhost_Id": {"type": "keyword"},
                    "showhost_Name": {"type": "keyword"},
                    "showvv_Id": {"type": "keyword"},
                    "showvv_Name": {"type": "keyword"},
                    "showpd_Id": {"type": "keyword"},
                    "showpd_Type": {"type": "keyword"},
                    "cmd": {"type": "keyword"},
                    "statvlun_IO_per_second_Cur": {"type": "float"},
                    "statvlun_IO_per_second_Avg": {"type": "float"},
                    "statvlun_IO_per_second_Max": {"type": "float"},
                    "statvlun_KBytes_per_sec_Cur": {"type": "float"},
                    "statvlun_KBytes_per_sec_Avg": {"type": "float"},
                    "statvlun_KBytes_per_sec_Max": {"type": "float"},
                    "statvlun_IOSz_KB_Cur": {"type": "float"},
                    "statvlun_IOSz_KB_Avg": {"type": "float"},
                    "statpd_IO_per_second_Cur": {"type": "float"},
                    "statpd_IO_per_second_Avg": {"type": "float"},
                    "statpd_IO_per_second_Max": {"type": "float"},
                    "statpd_KBytes_per_sec_Cur": {"type": "float"},
                    "statpd_KBytes_per_sec_Avg": {"type": "float"},
                    "statpd_KBytes_per_sec_Max": {"type": "float"},
                    "statpd_IOSz_KB_Cur": {"type": "float"},
                    "statpd_IOSz_KB_Avg": {"type": "float"},
                    "statpd_Idle_pct_Cur": {"type": "float"},
                    "statpd_Idle_pct_Avg": {"type": "float"},
                    "showvv_Usr_MB_Rsvd": {"type": "float"},
                    "showvv_Usr_MB_Used": {"type": "float"},
                    "showvv_Usr_pct_VSize_Used": {"type": "float"},
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