import paramiko
import re
import pprint
import datetime

class TPAR7K():
    def __init__(self, ssh_config):
        _ssh_config = ssh_config
        self.timestamp = '{0:%Y-%m-%d %H:%M}'.format(datetime.datetime.utcnow())

        try:
            _server = _ssh_config['server']
            _username = _ssh_config['username']
            _password = _ssh_config['password']
        except:
            print('ERR:[TPAR7K:__init__]: There is an error with one of ssh_config attributes')
        else:
            self.storage_name = _server
            self.ssh_client = paramiko.SSHClient()
            self.ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            self.ssh_client.connect(_server, username=_username, password=_password, look_for_keys=False)

    def _showvvs(self):
        _cmd_to_execute = 'showvv -s'
        _vvs = []

        _ssh_stdin, _ssh_stdout, _ssh_stderr = self.ssh_client.exec_command(_cmd_to_execute)
        _stdout = list(_ssh_stdout)

        """
        3PAR7KT cli% showvv -s
                                                       -----Adm----- ---------Snp---------- ---------------Usr---------------
                                                       ----(MB)----- --(MB)--- -(% VSize)-- -------(MB)-------- --(% VSize)-- -------(MB)-------- -Capacity Efficiency-
          Id Name                            Prov Type   Rsvd   Used Rsvd Used Used Wrn Lim      Rsvd      Used  Used Wrn Lim  Tot_Rsvd     VSize   Compaction    Dedup
           1 .srdata                         full base      0      0    0    0  0.0  --  --     61440     61440 100.0  --  --     61440     61440           --       --
           0 admin                           full base      0      0    0    0  0.0  --  --     10240     10240 100.0  --  --     10240     10240           --       --
         753 arya_disk                       tpvv base    384     86    0    0  0.0  --  --    137216    130540  85.0   0   0    137600    153600          1.2       --
        1603 aurum_data_atoll.0              tpvv base    256     49    0    0  0.0  --  --     77312     76732  99.9   0   0     77568     76800          1.0       --
        1604 aurum_data_atoll.1              tpvv base    256     49    0    0  0.0  --  --     77312     76725  99.9   0   0     77568     76800          1.0       --
        1682 aurum_data_atoll.2              tpvv base    256     49    0    0  0.0  --  --     77312     76667  99.8   0   0     77568     76800          1.0       --
        1605 aurum_data_dbstore.0            tpvv base    640    318    0    0  0.0  --  --    511872    511737  99.9   0   0    512512    512000          1.0       --
        """

        if _stdout:
            _headers = _stdout[2].strip()
            _pattern = re.compile(r'(?P<showvvs_Id>Id)\s+'
                                  r'(?P<showvvs_Name>Name)\s+'
                                  r'(?P<showvvs_Prov>Prov)\s+'
                                  r'(?P<showvvs_Type>Type)\s+'
                                  r'(?P<showvvs_Adm_Rsvd>Rsvd)\s+'
                                  r'(?P<showvvs_Adm_Used>Used)\s+'
                                  r'(?P<showvvs_Snp_MB_Rsvd>Rsvd)\s+'
                                  r'(?P<showvvs_Snp_MB_Used>Used)\s+'
                                  r'(?P<showvvs_Snp_pct_VSize_Used>Used)\s+'
                                  r'(?P<showvvs_Snp_pct_VSize_Wrn>Wrn)\s+'
                                  r'(?P<showvvs_Snp_pct_VSize_Lim>Lim)\s+'
                                  r'(?P<showvvs_Usr_MB_Rsvd>Rsvd)\s+'
                                  r'(?P<showvvs_Usr_MB_Used>Used)\s+'
                                  r'(?P<showvvs_Usr_pct_VSize_Used>Used)\s+'
                                  r'(?P<showvvs_Usr_pct_VSize_Wrn>Wrn)\s+'
                                  r'(?P<showvvs_Usr_pct_VSize_Lim>Lim)\s+'
                                  r'(?P<showvvs_MB_Tot_Rsvd>Tot_Rsvd)\s+'
                                  r'(?P<showvvs_MB_VSize>VSize)\s+'
                                  r'(?P<showvvs_Capacity_Efficiency_Compaction>Compaction)\s+'
                                  r'(?P<showvvs_Capacity_Efficiency_Dedup>Dedup)')
            try:
                _match = _pattern.match(_headers)
            except Exception as _ex:
                print('ERR: [TPAR7K:_showvvs]: There was an error occurred with headers re.match')
                print(_ex)
                return False
            else:
                _pattern = re.compile(r'(?P<showvvs_Id>\d+)\s+'
                                      r'(?P<showvvs_Name>\S+)\s+'
                                      r'(?P<showvvs_Prov>\S+)\s+'
                                      r'(?P<showvvs_Type>\S+)\s+'
                                      r'(?P<showvvs_Adm_Rsvd>\S+)\s+'
                                      r'(?P<showvvs_Adm_Used>\S+)\s+'
                                      r'(?P<showvvs_Snp_MB_Rsvd>\S+)\s+'
                                      r'(?P<showvvs_Snp_MB_Used>\S+)\s+'
                                      r'(?P<showvvs_Snp_pct_VSize_Used>\S+)\s+'
                                      r'(?P<showvvs_Snp_pct_VSize_Wrn>\S+)\s+'
                                      r'(?P<showvvs_Snp_pct_VSize_Lim>\S+)\s+'
                                      r'(?P<showvvs_Usr_MB_Rsvd>\S+)\s+'
                                      r'(?P<showvvs_Usr_MB_Used>\S+)\s+'
                                      r'(?P<showvvs_Usr_pct_VSize_Used>\S+)\s+'
                                      r'(?P<showvvs_Usr_pct_VSize_Wrn>\S+)\s+'
                                      r'(?P<showvvs_Usr_pct_VSize_Lim>\S+)\s+'
                                      r'(?P<showvvs_MB_Tot_Rsvd>\S+)\s+'
                                      r'(?P<showvvs_MB_VSize>\S+)\s+'
                                      r'(?P<showvvs_Capacity_Efficiency_Compaction>\S+)\s+'
                                      r'(?P<showvvs_Capacity_Efficiency_Dedup>\S+)')
                for _line in _stdout[3:]:
                    try:
                        _match = _pattern.match(_line.strip())
                    except:
                        print('WRN: [TPAR7K:_showvvs]: There was an error occurred with _line re.match')
                        print(_line.strip())
                    else:
                        if _match:
                            _vvs_dict = _match.groupdict()
                            _vvs_dict['timestamp'] = self.timestamp
                            _vvs_dict['cmd'] = 'showvvs'
                            _vvs_dict['storage_name'] = self.storage_name
                            _vvs.append(_vvs_dict)

        return _vvs

    def _showvvd(self):
        _cmd_to_execute = 'showvv -d'
        _vvd = []

        _ssh_stdin, _ssh_stdout, _ssh_stderr = self.ssh_client.exec_command(_cmd_to_execute)
        _stdout = list(_ssh_stdout)

        """
        3PAR7KT cli% showvv -d
          Id Name                            Rd Mstr  Prnt Roch Rwch PPrnt PBlkRemain -------------VV_WWN------------- ------CreationTime------ Udid
           1 .srdata                         RW 1/0/-  ---  ---  ---   ---         -- 60002AC0000000000000000100015038 2015-05-19 16:59:20 EEST    1
           0 admin                           RW 1/0/-  ---  ---  ---   ---         -- 60002AC0000000000000000000015038 2015-05-19 16:57:09 EEST    0
         753 arya_disk                       RW 1/0/-  ---  ---  ---   ---         -- 60002AC000000000000002F100015038 2016-09-28 16:51:15 EEST  753
        1603 aurum_data_atoll.0              RW 0/1/-  ---  ---  ---   ---         -- 60002AC0000000000000064300015038 2018-01-15 11:44:45 EET  1603
        1604 aurum_data_atoll.1              RW 0/1/-  ---  ---  ---   ---         -- 60002AC0000000000000064400015038 2018-01-15 11:44:46 EET  1604
        """

        if _stdout:
            _headers = _stdout[0].strip()
            _pattern = re.compile(r'(?P<showvvd_Id>Id)\s+'
                                  r'(?P<showvvd_Name>Name)\s+'
                                  r'(?P<showvvd_Rd>Rd)\s+'
                                  r'(?P<showvvd_Mstr>Mstr)\s+'
                                  r'(?P<showvvd_Prnt>Prnt)\s+'
                                  r'(?P<showvvd_Roch>Roch)\s+'
                                  r'(?P<showvvd_Rwch>Rwch)\s+'
                                  r'(?P<showvvd_PPrnt>PPrnt)\s+'
                                  r'(?P<showvvd_PBlkRemain>PBlkRemain)\s+'
                                  r'(?P<showvvd_VV_WWN>-------------VV_WWN-------------)\s+'
                                  r'(?P<showvvd_CreationTime>------CreationTime------)\s+'
                                  r'(?P<showvvd_Udid>Udid)')
            try:
                _match = _pattern.match(_headers)
            except Exception as _ex:
                print('ERR: [TPAR7K:_showvvd]: There was an error occurred with headers re.match')
                print(_ex)
                return False
            else:
                _pattern = re.compile(r'(?P<showvvd_Id>\d+)\s+'
                                      r'(?P<showvvd_Name>\S+)\s+'
                                      r'(?P<showvvd_Rd>\S+)\s+'
                                      r'(?P<showvvd_Mstr>\S+)\s+'
                                      r'(?P<showvvd_Prnt>\S+)\s+'
                                      r'(?P<showvvd_Roch>\S+)\s+'
                                      r'(?P<showvvd_Rwch>\S+)\s+'
                                      r'(?P<showvvd_PPrnt>\S+)\s+'
                                      r'(?P<showvvd_PBlkRemain>\S+)\s+'
                                      r'(?P<showvvd_VV_WWN>\S+)\s+'
                                      r'(?P<showvvd_CreationTime>\d+-\d+-\d+\s\d+:\d+:\d+\s\S+)\s+'
                                      r'(?P<showvvd_Udid>\d+)')
                for _line in _stdout[1:]:
                    try:
                        _match = _pattern.match(_line.strip())
                    except:
                        print('WRN: [TPAR7K:_showvvd]: There was an error occurred with _line re.match')
                        print(_line.strip())
                    else:
                        if _match:
                            _vvd_dict = _match.groupdict()
                            _vvd_dict['timestamp'] = self.timestamp
                            _vvd_dict['cmd'] = 'showvvd'
                            _vvd_dict['storage_name'] = self.storage_name
                            _vvd.append(_vvd_dict)

        return _vvd

    def _showpd(self):
        _cmd_to_execute = 'showpd -showcols Id,CagePos,Type,Size_MB,Free_MB,Volume_MB,Spare_MB,Failed_MB,Node_WWN,State,Capacity'
        _pds = []
        _ids = {}

        _ssh_stdin, _ssh_stdout, _ssh_stderr = self.ssh_client.exec_command(_cmd_to_execute)
        _stdout = list(_ssh_stdout)

        """
        3PAR7KT cli% showpd -showcols Id,CagePos,Type,Size_MB,Free_MB,Volume_MB,Spare_MB,Failed_MB,Node_WWN,State,Capacity
         Id CagePos Type   Size_MB  Free_MB Volume_MB Spare_MB Failed_MB         Node_WWN State  Capacity
          0  0:0:0  FC      838656   153600    649216    35840         0 5000CCA0578E48AB normal      900
        """

        if _stdout:
            _headers = _stdout[0].strip()
            _pattern = re.compile(r'(?P<showpd_Id>Id)\s+'
                                  r'(?P<showpd_CagePos>CagePos)\s+'
                                  r'(?P<showpd_Type>Type)\s+'
                                  r'(?P<showpd_Size_MB>Size_MB)\s+'
                                  r'(?P<showpd_Free_MB>Free_MB)\s+'
                                  r'(?P<showpd_Volume_MB>Volume_MB)\s+'
                                  r'(?P<showpd_Spare_MB>Spare_MB)\s+'
                                  r'(?P<showpd_Failed_MB>Failed_MB)\s+'
                                  r'(?P<showpd_Node_WWN>Node_WWN)\s+'
                                  r'(?P<showpd_State>State)\s+'
                                  r'(?P<showpd_Capacity>Capacity)')
            try:
                _match = _pattern.match(_headers)
            except Exception as _ex:
                print('ERR: [TPAR7K:_showpd]: There was an error occurred with re.match')
                print(_ex)
                return False
            else:
                _pattern = re.compile(r'(?P<showpd_Id>\d+)\s+'
                                      r'(?P<showpd_CagePos>.*)\s+'
                                      r'(?P<showpd_Type>\w+)\s+'
                                      r'(?P<showpd_Size_MB>\d+)\s+'
                                      r'(?P<showpd_Free_MB>\d+)\s+'
                                      r'(?P<showpd_Volume_MB>\d+)\s+'
                                      r'(?P<showpd_Spare_MB>\d+)\s+'
                                      r'(?P<showpd_Failed_MB>\d+)\s+'
                                      r'(?P<showpd_Node_WWN>.*)\s+'
                                      r'(?P<showpd_State>\w+)\s+'
                                      r'(?P<showpd_Capacity>\d+)')
                for _line in _stdout[1:]:
                    try:
                        _match = _pattern.match(_line.strip())
                    except:
                        print('WRN: [TPAR7K:_showpd]: There was an error occurred with _line re.match')
                        print(_line.strip())
                    else:
                        if _match:
                            _pd_dict = _match.groupdict()
                            _pd_dict['timestamp'] = self.timestamp
                            _pd_dict['storage_name'] = self.storage_name
                            _pd_dict['cmd'] = 'showpd'
                            _pds.append(_pd_dict)
                            if _pd_dict['showpd_Id'] not in _ids:
                                _ids[_pd_dict['showpd_Id']] = 1

        return (_pds, _ids.keys())

    def _showld(self):
        _cmd_to_execute = 'showld -state'
        _lds = []

        _ssh_stdin, _ssh_stdout, _ssh_stderr = self.ssh_client.exec_command(_cmd_to_execute)
        _stdout = list(_ssh_stdout)

        """
        3PAR7KT cli% showld -state tp-4-sd-0.43
          Id Name         -State- -Detailed_State-
        6517 tp-4-sd-0.43 normal  normal
        ------------------------------------------
           1
        """

        if _stdout:
            _headers = _stdout[0].strip()
            _pattern = re.compile(r'(?P<showld_Id>Id)\s+'
                                  r'(?P<showld_Name>Name)\s+'
                                  r'(?P<showld_State>-State-)\s+'
                                  r'(?P<showld_Detailed_State>-Detailed_State-)')
            try:
                _match = _pattern.match(_headers)
            except Exception as _ex:
                print('ERR: [TPAR7K:_showld]: There was an error occurred with headers re.match')
                print(_ex)
                return False
            else:
                _pattern = re.compile(r'(?P<showld_Id>\S+)\s+'
                                      r'(?P<showld_Name>\S+)\s+'
                                      r'(?P<showld_State>\S+)\s+'
                                      r'(?P<showld_Detailed_State>\S+)')
                for _line in _stdout[1:]:
                    try:
                        _match = _pattern.match(_line.strip())
                    except:
                        print('WRN: [TPAR7K:_showld]: There was an error occurred with _line re.match')
                        print(_line.strip())
                    else:
                        if _match:
                            _lds_dict = _match.groupdict()
                            _lds_dict['timestamp'] = self.timestamp
                            _lds_dict['cmd'] = 'showld'
                            _lds_dict['storage_name'] = self.storage_name
                            _lds.append(_lds_dict)

        return _lds

    def _showhost(self):
        _cmd_to_execute = 'showhost -d'
        _hosts = []

        _ssh_stdin, _ssh_stdout, _ssh_stderr = self.ssh_client.exec_command(_cmd_to_execute)
        _stdout = list(_ssh_stdout)

        """
        3PAR7KT cli% showhost -d
         Id Name                 Persona       -WWN/iSCSI_Name- Port  IP_addr
          8 ants                 Generic-ALUA  5001438024D2B6E2 1:1:2 n/a
          8 ants                 Generic-ALUA  5001438024D2B6E0 1:1:1 n/a
          8 ants                 Generic-ALUA  5001438024D2B6E0 0:1:1 n/a
          8 ants                 Generic-ALUA  5001438024D2B6E2 0:1:2 n/a
          0 ares                 HPUX          5001438003B9E4F2 0:1:2 n/a
        """

        if _stdout:
            _headers = _stdout[0].strip()
            _pattern = re.compile(r'(?P<showhost_Id>Id)\s+'
                                  r'(?P<showhost_Name>Name)\s+'
                                  r'(?P<showhost_Persona>Persona)\s+'
                                  r'(?P<showhost_WWN>-WWN/iSCSI_Name-)\s+'
                                  r'(?P<showhost_Port>Port)\s+'
                                  r'(?P<showhost_IP_addr>IP_addr)')
            try:
                _match = _pattern.match(_headers)
            except Exception as _ex:
                print('ERR: [TPAR7K:_showhost]: There was an error occurred with headers re.match')
                print(_ex)
                return False
            else:
                _pattern = re.compile(r'(?P<showhost_Id>\d+)\s+'
                                      r'(?P<showhost_Name>\S+)\s+'
                                      r'(?P<showhost_Persona>\S+)\s+'
                                      r'(?P<showhost_WWN>\S+)\s+'
                                      r'(?P<showhost_Port>\S+)\s+'
                                      r'(?P<showhost_IP_addr>\S+)')
                for _line in _stdout[1:]:
                    try:
                        _match = _pattern.match(_line.strip())
                    except:
                        print('WRN: [TPAR7K:_showhost]: There was an error occurred with _line re.match')
                        print(_line.strip())
                    else:
                        if _match:
                            _hosts_dict = _match.groupdict()
                            _hosts_dict['timestamp'] = self.timestamp
                            _hosts_dict['cmd'] = 'showhost'
                            _hosts_dict['storage_name'] = self.storage_name
                            _hosts.append(_hosts_dict)

        return _hosts

    def _showpdvv(self, ids):
        _ids = sorted([int(_i) for _i in list(ids)])
        _cmd_to_execute = 'showpdvv -p -dk ' + str(_ids[0]) + '-' +str(_ids[-1])
        print(_cmd_to_execute)
        _pdvvs = []

        _ssh_stdin, _ssh_stdout, _ssh_stderr = self.ssh_client.exec_command(_cmd_to_execute)
        _stdout = list(_ssh_stdout)

        """
        3PAR7KT cli% showpdvv -p -dk 4-5
        PDId CagePos Type RPM VVId VVName                     VVSp
           4 0:4:0   FC    10    0 admin                      usr
           4 0:4:0   FC    10    3 sin_utu_testrestores       usr
           4 0:4:0   FC    10    6 utu_leonardo_dp_test       usr
           4 0:4:0   FC    10  261 hpeesxi_data.0             usr
        """

        if _stdout:
            _headers = _stdout[0].strip()
            _pattern = re.compile(r'(?P<showpdvv_PDId>PDId)\s+'
                                  r'(?P<showpdvv_CagePos>CagePos)\s+'
                                  r'(?P<showpdvv_Type>Type)\s+'
                                  r'(?P<showpdvv_RPM>RPM)\s+'
                                  r'(?P<showpdvv_VVId>VVId)\s+'
                                  r'(?P<showpdvv_VVName>VVName)\s+'
                                  r'(?P<showpdvv_VVSp>VVSp)')
            try:
                _match = _pattern.match(_headers)
            except Exception as _ex:
                print('ERR: [TPAR7K:_showpdvv]: There was an error occurred with headers re.match')
                print(_ex)
                return False
            else:
                _pattern = re.compile(r'(?P<showpdvv_PDId>\d+)\s+'
                                      r'(?P<showpdvv_CagePos>\S+)\s+'
                                      r'(?P<showpdvv_Type>\w+)\s+'
                                      r'(?P<showpdvv_RPM>\d+)\s+'
                                      r'(?P<showpdvv_VVId>\d+)\s+'
                                      r'(?P<showpdvv_VVName>\S+)\s+'
                                      r'(?P<showpdvv_VVSp>\S+)')
                for _line in _stdout[1:]:
                    try:
                        _match = _pattern.match(_line.strip())
                    except:
                        print('WRN: [TPAR7K:_showpdvv]: There was an error occurred with _line re.match')
                        print(_line.strip())
                    else:
                        if _match:
                            _pdvvs_dict = _match.groupdict()
                            _pdvvs_dict['timestamp'] = self.timestamp
                            _pdvvs_dict['cmd'] = 'showpdvv'
                            _pdvvs_dict['storage_name'] = self.storage_name
                            _pdvvs.append(_pdvvs_dict)

        return _pdvvs

    def _showvlun(self):
        _cmd_to_execute = 'showvlun -a -showcols Lun,VVName,HostName,Host_WWN,Port,VV_WWN,Status'
        _vluns = []

        _ssh_stdin, _ssh_stdout, _ssh_stderr = self.ssh_client.exec_command(_cmd_to_execute)
        _stdout = list(_ssh_stdout)

        """
        3PAR7KT cli% showvlun -a -showcols Lun,VVName,HostName,Host_WWN,Port,VV_WWN,Status -host sin
        Lun VVName               HostName Host_WWN         Port  VV_WWN                           Status
          1 sin_utu_testrestores sin      500143800568F222 0:1:2 60002AC0000000000000000300015038 active
          1 sin_utu_testrestores sin      500143800568F222 1:1:2 60002AC0000000000000000300015038 active
          1 sin_utu_testrestores sin      500143800568F220 1:1:1 60002AC0000000000000000300015038 active
          1 sin_utu_testrestores sin      500143800568F220 0:1:1 60002AC0000000000000000300015038 active
        ------------------------------------------------------------------------------------------------
          4 total
        """

        if _stdout:
            _headers = _stdout[0].strip()
            _pattern = re.compile(r'(?P<showvlun_Lun>Lun)\s+'
                                  r'(?P<showvlun_VVName>VVName)\s+'
                                  r'(?P<showvlun_HostName>HostName)\s+'
                                  r'(?P<showvlun_Host_WWN>Host_WWN)\s+'
                                  r'(?P<showvlun_Port>Port)\s+'
                                  r'(?P<showvlun_VV_WWN>VV_WWN)\s+'
                                  r'(?P<showvlun_Status>Status)')
            try:
                _match = _pattern.match(_headers)
            except Exception as _ex:
                print('ERR: [TPAR7K:_showvlun]: There was an error occurred with re.match')
                print(_ex)
                return False
            else:
                _pattern = re.compile(r'(?P<showvlun_Lun>\d+)\s+'
                                      r'(?P<showvlun_VVName>\S+)\s+'
                                      r'(?P<showvlun_HostName>\S+)\s+'
                                      r'(?P<showvlun_Host_WWN>\S+)\s+'
                                      r'(?P<showvlun_Port>\S+)\s+'
                                      r'(?P<showvlun_VV_WWN>\S+)\s+'
                                      r'(?P<showvlun_Status>\S+)')
                for _line in _stdout[1:]:
                    try:
                        _match = _pattern.match(_line.strip())
                    except:
                        print('WRN: [TPAR7K:_showvlun]: There was an error occurred with _line re.match')
                        print(_line.strip())
                    else:
                        if _match:
                            _vlun_dict = _match.groupdict()
                            _vlun_dict['timestamp'] = self.timestamp
                            _vlun_dict['storage_name'] = self.storage_name
                            _vlun_dict['cmd'] = 'showvlun'
                            _vluns.append(_vlun_dict)

        return _vluns

    def _statcpu(self):
        _cmd_to_execute = 'statcpu -iter 1'
        _cpu = []

        _ssh_stdin, _ssh_stdout, _ssh_stderr = self.ssh_client.exec_command(_cmd_to_execute)
        _stdout = list(_ssh_stdout)

        """
        3PAR7KT cli% statcpu -iter 1
        13:27:00 04/17/2020
        node,cpu user sys idle intr/s ctxt/s
             0,0   11   4   85
             0,1    1  10   89
             0,2    1   9   90
             0,3    0   2   98
             0,4    0   9   91
             0,5    1   4   96
             0,6    0  16   84
             0,7    1   1   98
        0,total    2   7   91  37737  32768
        
             1,0    1   2   97
             1,1    3   8   89
             1,2    1   4   96
             1,3    2   1   98
             1,4    0   7   93
             1,5    0   4   96
             1,6    1  11   88
             1,7    0   0   99
        1,total    1   5   95  35094  25802
        """

        if _stdout:
            _headers = _stdout[1].strip()
            _pattern = re.compile(r'(?P<statcpu_node>node),'
                                  r'(?P<statcpu_cpu>cpu)\s+'
                                  r'(?P<statcpu_user>user)\s+'
                                  r'(?P<statcpu_sys>sys)\s+'
                                  r'(?P<statcpu_idle>idle)\s+'
                                  r'(?P<statcpu_intr>intr/s)\s+'
                                  r'(?P<statcpu_ctxt>ctxt/s)')
            try:
                _match = _pattern.match(_headers)
            except Exception as _ex:
                print('ERR: [TPAR7K:_statcpu]: There was an error occurred with re.match')
                print(_ex)
                return False
            else:
                _pattern = re.compile(r'(?P<statcpu_node>\d+),'
                                      r'(?P<statcpu_cpu>total)\s+'
                                      r'(?P<statcpu_user>\d+)\s+'
                                      r'(?P<statcpu_sys>\d+)\s+'
                                      r'(?P<statcpu_idle>\d+)\s+'
                                      r'(?P<statcpu_intr>\d+)\s+'
                                      r'(?P<statcpu_ctxt>\d+)')
                for _line in _stdout[1:]:
                    try:
                        _match = _pattern.match(_line.strip())
                    except:
                        print('WRN: [TPAR7K:_statcpu]: There was an error occurred with _line re.match')
                        print(_line.strip())
                    else:
                        if _match:
                            _cpu_dict = _match.groupdict()
                            _cpu_dict['timestamp'] = self.timestamp
                            _cpu_dict['cmd'] = 'statcpu'
                            _cpu_dict['storage_name'] = self.storage_name
                            _cpu.append(_cpu_dict)

        return _cpu

    def _statpd(self):
        _cmd_to_execute = 'statpd -d 5 -iter 1'
        _pds = []

        _ssh_stdin, _ssh_stdout, _ssh_stderr = self.ssh_client.exec_command(_cmd_to_execute)
        _stdout = list(_ssh_stdout)

        """
        3PAR7KT cli% statpd -iter 1
        12:22:37 04/11/2020 r/w  I/O per second     KBytes per sec      Svt ms     IOSz KB       Idle %
              ID       Port       Cur   Avg Max    Cur    Avg  Max   Cur   Avg   Cur   Avg Qlen Cur Avg
               0      1:0:1   t    13    13  13    575    575  575  9.67  9.67  45.6  45.6    0  95  95
               1      0:0:1   t     5     5   5    423    423  423 15.74 15.74  93.2  93.2    0  98  98
               2      1:0:1   t     5     5   5    382    382  382  7.80  7.80  80.6  80.6    0  97  97
        """

        if _stdout:
            _headers = _stdout[1].strip()
            _pattern = re.compile(r'(?P<statpd_ID>ID)\s+'
                                  r'(?P<statpd_Port>Port)\s+'
                                  r'(?P<statpd_IO_per_second_Cur>Cur)\s+'
                                  r'(?P<statpd_IO_per_second_Avg>Avg)\s+'
                                  r'(?P<statpd_IO_per_second_Max>Max)\s+'
                                  r'(?P<statpd_KBytes_per_sec_Cur>Cur)\s+'
                                  r'(?P<statpd_KBytes_per_sec_Avg>Avg)\s+'
                                  r'(?P<statpd_KBytes_per_sec_Max>Max)\s+'
                                  r'(?P<statpd_Svt_ms_Cur>Cur)\s+'
                                  r'(?P<statpd_Svt_ms_Avg>Avg)\s+'
                                  r'(?P<statpd_IOSz_KB_Cur>Cur)\s+'
                                  r'(?P<statpd_IOSz_KB_Avg>Avg)\s+'
                                  r'(?P<statpd_Qlen>Qlen)\s+'
                                  r'(?P<statpd_Idle_pct_Cur>Cur)\s+'
                                  r'(?P<statpd_Idle_pct_Avg>Avg)')
            try:
                _match = _pattern.match(_headers)
            except Exception as _ex:
                print('ERR: [TPAR7K:_statpd]: There was an error occurred with headers re.match')
                print(_ex)
                return False
            else:
                _pattern = re.compile(r'(?P<statpd_ID>\d+)\s+'
                                      r'(?P<statpd_Port>\S+)\s+'
                                      r'(?P<statpd_r_w>\S+)\s+'
                                      r'(?P<statpd_IO_per_second_Cur>\S+)\s+'
                                      r'(?P<statpd_IO_per_second_Avg>\S+)\s+'
                                      r'(?P<statpd_IO_per_second_Max>\S+)\s+'
                                      r'(?P<statpd_KBytes_per_sec_Cur>\S+)\s+'
                                      r'(?P<statpd_KBytes_per_sec_Avg>\S+)\s+'
                                      r'(?P<statpd_KBytes_per_sec_Max>\S+)\s+'
                                      r'(?P<statpd_Svt_ms_Cur>\S+)\s+'
                                      r'(?P<statpd_Svt_ms_Avg>\S+)\s+'
                                      r'(?P<statpd_IOSz_KB_Cur>\S+)\s+'
                                      r'(?P<statpd_IOSz_KB_Avg>\S+)\s+'
                                      r'(?P<statpd_Qlen>\S+)\s+'
                                      r'(?P<statpd_Idle_pct_Cur>\d+)\s+'
                                      r'(?P<statpd_Idle_pct_Avg>\d+)')
                for _line in _stdout[1:]:
                    try:
                        _match = _pattern.match(_line.strip())
                    except:
                        print('WRN: [TPAR7K:_statpd]: There was an error occurred with _line re.match')
                        print(_line.strip())
                    else:
                        if _match:
                            _pds_dict = _match.groupdict()
                            _pds_dict['timestamp'] = self.timestamp
                            _pds_dict['cmd'] = 'statpd'
                            _pds_dict['storage_name'] = self.storage_name
                            _pds.append(_pds_dict)

        return _pds

    def _statvlun(self):
        _cmd_to_execute = 'statvlun -iter 1'
        _vluns = []

        _ssh_stdin, _ssh_stdout, _ssh_stderr = self.ssh_client.exec_command(_cmd_to_execute)
        _stdout = list(_ssh_stdout)

        """
        3PAR7KT cli% statvlun -d 5 -host italy -iter 1
              12:07:30 04/11/2020 r/w I/O per second KBytes per sec    Svt ms IOSz KB
        Lun            VVname  Host  Port      Cur  Avg  Max  Cur  Avg  Max  Cur  Avg Cur Avg Qlen
          0 italy_test_disk.4 italy 0:2:1   t    0    0    0    0    0    0 0.00 0.00 0.0 0.0    0
          1 italy_test_disk.3 italy 0:2:1   t    0    0    0    0    0    0 0.00 0.00 0.0 0.0    0
          2 italy_test_disk.2 italy 0:2:1   t    0    0    0    0    0    0 0.00 0.00 0.0 0.0    0
        """

        if _stdout:
            _headers = _stdout[1].strip()
            _pattern = re.compile(r'(?P<statvlun_Lun>Lun)\s+'
                                  r'(?P<statvlun_VVname>VVname)\s+'
                                  r'(?P<statvlun_Host>Host)\s+'
                                  r'(?P<statvlun_Port>Port)\s+'
                                  r'(?P<statvlun_IO_per_second_Cur>Cur)\s+'
                                  r'(?P<statvlun_IO_per_second_Avg>Avg)\s+'
                                  r'(?P<statvlun_IO_per_second_Max>Max)\s+'
                                  r'(?P<statvlun_KBytes_per_sec_Cur>Cur)\s+'
                                  r'(?P<statvlun_KBytes_per_sec_Avg>Avg)\s+'
                                  r'(?P<statvlun_KBytes_per_sec_Max>Max)\s+'
                                  r'(?P<statvlun_Svt_ms_Cur>Cur)\s+'
                                  r'(?P<statvlun_Svt_ms_Avg>Avg)\s+'
                                  r'(?P<statvlun_IOSz_KB_Cur>Cur)\s+'
                                  r'(?P<statvlun_IOSz_KB_Avg>Avg)\s+'
                                  r'(?P<statvlun_Qlen>Qlen)')
            try:
                _match = _pattern.match(_headers)
            except Exception as _ex:
                print('ERR: [TPAR7K:_statvlun]: There was an error occurred with headers re.match')
                print(_ex)
                return False
            else:
                _pattern = re.compile(r'(?P<statvlun_Lun>\d+)\s+'
                                      r'(?P<statvlun_VVname>\S+)\s+'
                                      r'(?P<statvlun_Host>\S+)\s+'
                                      r'(?P<statvlun_Port>\S+)\s+'
                                      r'(?P<statvlun_rw>\S+)\s+'
                                      r'(?P<statvlun_IO_per_second_Cur>\S+)\s+'
                                      r'(?P<statvlun_IO_per_second_Avg>\S+)\s+'
                                      r'(?P<statvlun_IO_per_second_Max>\S+)\s+'
                                      r'(?P<statvlun_KBytes_per_sec_Cur>\S+)\s+'
                                      r'(?P<statvlun_KBytes_per_sec_Avg>\S+)\s+'
                                      r'(?P<statvlun_KBytes_per_sec_Max>\S+)\s+'
                                      r'(?P<statvlun_Svt_ms_Cur>\S+)\s+'
                                      r'(?P<statvlun_Svt_ms_Avg>\S+)\s+'
                                      r'(?P<statvlun_IOSz_KB_Cur>\S+)\s+'
                                      r'(?P<statvlun_IOSz_KB_Avg>\S+)\s+'
                                      r'(?P<statvlun_Qlen>\S+)')
                for _line in _stdout[1:]:
                    try:
                        _match = _pattern.match(_line.strip())
                    except:
                        print('WRN: [TPAR7K:_statvlun]: There was an error occurred with _line re.match')
                        print(_line.strip())
                    else:
                        if _match:
                            _vluns_dict = _match.groupdict()
                            _vluns_dict['timestamp'] = self.timestamp
                            _vluns_dict['cmd'] = 'statvlun'
                            _vluns_dict['storage_name'] = self.storage_name
                            _vluns.append(_vluns_dict)

        return _vluns

    def _statld(self):
        _cmd_to_execute = 'statld -iter 1'
        _lds = []

        _ssh_stdin, _ssh_stdout, _ssh_stderr = self.ssh_client.exec_command(_cmd_to_execute)
        _stdout = list(_ssh_stdout)

        """
        3PAR7KT cli% statld -iter 1 tp-0-sd-0.496
        14:13:48 04/22/2020 r/w I/O per second KBytes per sec    Svt ms IOSz KB
                     Ldname      Cur  Avg  Max  Cur  Avg  Max  Cur  Avg Cur Avg Qlen
              tp-0-sd-0.496   t    0    0    0    0    0    0 0.00 0.00 0.0 0.0    0
        ----------------------------------------------------------------------------
                          1   t    0    0         0    0      0.00 0.00 0.0 0.0    0
        """

        if _stdout:
            _headers = _stdout[1].strip()
            _pattern = re.compile(r'(?P<statld_Ldname>Ldname)\s+'
                                  r'(?P<statld_IO_per_second_Cur>Cur)\s+'
                                  r'(?P<statld_IO_per_second_Avg>Avg)\s+'
                                  r'(?P<statld_IO_per_second_Max>Max)\s+'
                                  r'(?P<statld_KBytes_per_sec_Cur>Cur)\s+'
                                  r'(?P<statld_KBytes_per_sec_Avg>Avg)\s+'
                                  r'(?P<statld_KBytes_per_sec_Max>Max)\s+'
                                  r'(?P<statld_Svt_ms_Cur>Cur)\s+'
                                  r'(?P<statld_Svt_ms_Avg>Avg)\s+'
                                  r'(?P<statld_IOSz_KB_Cur>Cur)\s+'
                                  r'(?P<statld_IOSz_KB_Avg>Avg)\s+'
                                  r'(?P<statld_Qlen>Qlen)')
            try:
                _match = _pattern.match(_headers)
            except Exception as _ex:
                print('ERR: [TPAR7K:_statld]: There was an error occurred with headers re.match')
                print(_ex)
                return False
            else:
                _pattern = re.compile(r'(?P<statld_Ldname>\S+)\s+'
                                      r'(?P<statld_rw>\S+)\s+'
                                      r'(?P<statld_IO_per_second_Cur>\S+)\s+'
                                      r'(?P<statld_IO_per_second_Avg>\S+)\s+'
                                      r'(?P<statld_IO_per_second_Max>\S+)\s+'
                                      r'(?P<statld_KBytes_per_sec_Cur>\S+)\s+'
                                      r'(?P<statld_KBytes_per_sec_Avg>\S+)\s+'
                                      r'(?P<statld_KBytes_per_sec_Max>\S+)\s+'
                                      r'(?P<statld_Svt_ms_Cur>\S+)\s+'
                                      r'(?P<statld_Svt_ms_Avg>\S+)\s+'
                                      r'(?P<statld_IOSz_KB_Cur>\S+)\s+'
                                      r'(?P<statld_IOSz_KB_Avg>\S+)\s+'
                                      r'(?P<statld_Qlen>\S+)')
                for _line in _stdout[1:]:
                    try:
                        _match = _pattern.match(_line.strip())
                    except:
                        print('WRN: [TPAR7K:_statld]: There was an error occurred with _line re.match')
                        print(_line.strip())
                    else:
                        if _match:
                            _lds_dict = _match.groupdict()
                            _lds_dict['timestamp'] = self.timestamp
                            _lds_dict['storage_name'] = self.storage_name
                            _lds_dict['cmd'] = 'statld'
                            _lds.append(_lds_dict)

        return _lds

    def _showsys(self):
        _cmds = {
            "FC": "showsys -space -devtype FC",
            "NL": "showsys -space -devtype NL",
            "SSD": "showsys -space -devtype SSD",
            "ALL": "showsys -space"
        }
        _sys = []

        for _devtype in _cmds:
            _ssh_stdin, _ssh_stdout, _ssh_stderr = self.ssh_client.exec_command(_cmds[_devtype])
            _stdout = list(_ssh_stdout)

            """
                    3PAR7KT cli% showsys -space
            ------------- System Capacity (MB) --------------
            Total Capacity                      :   401719296
              Allocated                         :   338138112
                Volumes                         :   325562368
                  Non-CPGs                      :           0
                    User                        :           0
                    Snapshot                    :           0
                    Admin                       :           0
                  CPGs (TPVVs & TDVVs & CPVVs)  :   325562368
                    User                        :   324704195
                      Used                      :   324653305
                      Used (Bulk VVs)           :           0
                      Unused                    :       50890
                    Snapshot                    :      120893
                      Used                      :           0
                      Used (Bulk VVs)           :           0
                      Unused                    :      120893
                    Admin                       :      737280
                      Used                      :      660864
                      Used (Bulk VVs)           :           0
                      Unused                    :       76416
                  Unmapped                      :           0
                System                          :    12321792
                  Internal                      :      130048
                  Spare                         :    12191744
                    Used                        :           0
                    Unused                      :    12191744
              Free                              :    63577088
                Initialized                     :    63577088
                Uninitialized                   :           0
              Unavailable                       :           0
              Failed                            :        4096
            logging failed, result = logtask : Permission denied
            -------------- Capacity Efficiency --------------
            logging failed, result = logtask : Permission denied
            Compaction                          :         1.1
            logging failed, result = logtask : Permission denied
            Dedup                               :   ---------
            """

            if _stdout:
                _i = 0
                _stdout_dict = {}
                _sys_dict = {}
                _pattern = re.compile(r'(?P<key>.*?):\s+(?P<value>\d+)')
                for _line in _stdout[1:-7]:
                    try:
                        _match = _pattern.match(_line.strip())
                    except:
                        print('WRN: [TPAR7K:_showsys]: There was an error occurred with _line re.match')
                        print(_line.strip().strip())
                    else:
                        if _match:
                            _groupdict = _match.groupdict()
                            _key = (_groupdict['key']).strip()
                            _value = (_groupdict['value']).strip()
                            _stdout_dict[_i] = {_key: _value}
                    _i += 1

                _sys_dict['showsys_Total_Capacity'] = _stdout_dict[0]['Total Capacity']
                _sys_dict['showsys_Allocated'] = _stdout_dict[1]['Allocated']
                _sys_dict['showsys_devtype'] = _devtype
                _sys_dict['timestamp'] = self.timestamp
                _sys_dict['storage_name'] = self.storage_name
                _sys_dict['cmd'] = 'showsys'
                _sys.append(_sys_dict)

        return _sys

    def getdata(self):
        _summ = []
        _pp = pprint.PrettyPrinter(indent=4)

        (_res_showpd, _res_showpd_ids) = self._showpd()
        _res_showld = self._showld()
        _res_statpd = self._statpd()
        _res_showpdvv = self._showpdvv(ids=_res_showpd_ids)
        _res_showvvd = self._showvvd()
        _res_showvvs = self._showvvs()
        _res_showhost = self._showhost()
        _res_showvlun = self._showvlun()
        _res_statvlun = self._statvlun()
        _res_statcpu = self._statcpu()
        _res_statld = self._statld()
        _res_showsys = self._showsys()

        for _list in [_res_showpd, _res_showld, _res_showpdvv, _res_showvvd, _res_showvvs, _res_showhost, _res_showvlun,
                   _res_statvlun, _res_statcpu, _res_statpd, _res_statld, _res_showsys]:
            _summ += _list

        return(_summ)