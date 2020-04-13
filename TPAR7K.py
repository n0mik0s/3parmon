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
            print('ERR: There is an error with ssh_config attribute in __init__ method')
        else:
            self.ssh_client = paramiko.SSHClient()
            self.ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            self.ssh_client.connect(_server, username=_username, password=_password, look_for_keys=False)

    def _showpd(self):
        _cmd_to_execute = 'showpd -showcols Id,CagePos,Type,Size_MB,Free_MB,Volume_MB,Spare_MB,Failed_MB,Node_WWN,State,Capacity'
        _pds = {}

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
                            _pd_dict['cmd'] = 'showpd'
                            _pds[_pd_dict['showpd_Id']] = _pd_dict

        return _pds

    def _showvv(self):
        _cmd_to_execute = 'showvv -s'
        _vvs = {}

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
            _pattern = re.compile(r'(?P<showvv_Id>Id)\s+'
                                  r'(?P<showvv_Name>Name)\s+'
                                  r'(?P<showvv_Prov>Prov)\s+'
                                  r'(?P<showvv_Type>Type)\s+'
                                  r'(?P<showvv_Adm_Rsvd>Rsvd)\s+'
                                  r'(?P<showvv_Adm_Used>Used)\s+'
                                  r'(?P<showvv_Snp_MB_Rsvd>Rsvd)\s+'
                                  r'(?P<showvv_Snp_MB_Used>Used)\s+'
                                  r'(?P<showvv_Snp_pct_VSize_Used>Used)\s+'
                                  r'(?P<showvv_Snp_pct_VSize_Wrn>Wrn)\s+'
                                  r'(?P<showvv_Snp_pct_VSize_Lim>Lim)\s+'
                                  r'(?P<showvv_Usr_MB_Rsvd>Rsvd)\s+'
                                  r'(?P<showvv_Usr_MB_Used>Used)\s+'
                                  r'(?P<showvv_Usr_pct_VSize_Used>Used)\s+'
                                  r'(?P<showvv_Usr_pct_VSize_Wrn>Wrn)\s+'
                                  r'(?P<showvv_Usr_pct_VSize_Lim>Lim)\s+'
                                  r'(?P<showvv_MB_Tot_Rsvd>Tot_Rsvd)\s+'
                                  r'(?P<showvv_MB_VSize>VSize)\s+'
                                  r'(?P<showvv_Capacity_Efficiency_Compaction>Compaction)\s+'
                                  r'(?P<showvv_Capacity_Efficiency_Dedup>Dedup)')
            try:
                _match = _pattern.match(_headers)
            except Exception as _ex:
                print('ERR: [TPAR7K:_showvv]: There was an error occurred with headers re.match')
                print(_ex)
                return False
            else:
                _pattern = re.compile(r'(?P<showvv_Id>\d+)\s+'
                                      r'(?P<showvv_Name>\S+)\s+'
                                      r'(?P<showvv_Prov>\S+)\s+'
                                      r'(?P<showvv_Type>\S+)\s+'
                                      r'(?P<showvv_Adm_Rsvd>\S+)\s+'
                                      r'(?P<showvv_Adm_Used>\S+)\s+'
                                      r'(?P<showvv_Snp_MB_Rsvd>\S+)\s+'
                                      r'(?P<showvv_Snp_MB_Used>\S+)\s+'
                                      r'(?P<showvv_Snp_pct_VSize_Used>\S+)\s+'
                                      r'(?P<showvv_Snp_pct_VSize_Wrn>\S+)\s+'
                                      r'(?P<showvv_Snp_pct_VSize_Lim>\S+)\s+'
                                      r'(?P<showvv_Usr_MB_Rsvd>\S+)\s+'
                                      r'(?P<showvv_Usr_MB_Used>\S+)\s+'
                                      r'(?P<showvv_Usr_pct_VSize_Used>\S+)\s+'
                                      r'(?P<showvv_Usr_pct_VSize_Wrn>\S+)\s+'
                                      r'(?P<showvv_Usr_pct_VSize_Lim>\S+)\s+'
                                      r'(?P<showvv_MB_Tot_Rsvd>\S+)\s+'
                                      r'(?P<showvv_MB_VSize>\S+)\s+'
                                      r'(?P<showvv_Capacity_Efficiency_Compaction>\S+)\s+'
                                      r'(?P<showvv_Capacity_Efficiency_Dedup>\S+)')
                for _line in _stdout[3:]:
                    try:
                        _match = _pattern.match(_line.strip())
                    except:
                        print('WRN: [TPAR7K:_showvv]: There was an error occurred with _line re.match')
                        print(_line.strip())
                    else:
                        if _match:
                            _vvs_dict = _match.groupdict()
                            _vvs_dict['timestamp'] = self.timestamp
                            _vvs_dict['cmd'] = 'showvv'
                            _vvs[_vvs_dict['showvv_Id']] = _vvs_dict

        return _vvs

    def _showhost(self):
        _cmd_to_execute = 'showhost -d'
        _hosts = {}

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
                            if _hosts_dict['showhost_Name'] not in _hosts:
                                _hosts[_hosts_dict['showhost_Name']] = _hosts_dict
                            else:
                                _hosts[_hosts_dict['showhost_Name']]['showhost_WWN'] = ';'.join([_hosts[_hosts_dict['showhost_Name']]['showhost_WWN'], _hosts_dict['showhost_WWN']])

        return _hosts

    def _statvlun(self):
        _cmd_to_execute = 'statvlun -d 5 -iter 1'
        _vluns = {}

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
                            _vluns[_vluns_dict['statvlun_VVname']] = _vluns_dict

        return _vluns

    def _statpd(self):
        _cmd_to_execute = 'statpd -d 5 -iter 1'
        _pds = {}

        _ssh_stdin, _ssh_stdout, _ssh_stderr = self.ssh_client.exec_command(_cmd_to_execute)
        _stdout = list(_ssh_stdout)

        """
        3PAR7KT cli% statpd -d 5 -iter 1
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
                            _pds[_pds_dict['statpd_ID']] = _pds_dict

        return _pds

    def _showpdvv(self, pds):
        _pds = sorted([int(x) for x in pds.keys()])
        _cmd_to_execute = 'showpdvv -p -dk ' + str(_pds[0]) + '-' +str(_pds[-1])

        _pdvvs = {}

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
                _i = 0
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
                            _pdvvs[_i] = _pdvvs_dict
                    _i += 1

        return _pdvvs

    def getdata(self):
        _summ = []
        _pp = pprint.PrettyPrinter(indent=4)

        _pds = self._showpd()
        _pdvvs = self._showpdvv(pds=_pds)
        _vvs = self._showvv()
        _hosts = self._showhost()
        _vluns = self._statvlun()
        _pd = self._statpd()

        for _d in [_pds, _pdvvs, _vvs, _hosts, _vluns, _pd]:
            _summ += _d.values()

        return(_summ)