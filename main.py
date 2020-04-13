import TPAR7K
import argparse
import os
import pprint
import yaml
import datetime
import es

if __name__=="__main__":
    startTime = datetime.datetime.now()
    pp = pprint.PrettyPrinter(indent=4)

    argparser = argparse.ArgumentParser(usage='%(prog)s [options]')
    argparser.add_argument('-c', '--conf',
                           help='Set full path to the configuration file.',
                           default='conf.yml')
    argparser.add_argument('-v', '--verbose',
                           help='Set verbose run to true.',
                           action='store_true')

    args = argparser.parse_args()

    verbose = args.verbose
    root_dir = os.path.dirname(os.path.realpath(__file__))
    conf_path_full = str(root_dir) + os.sep + str(args.conf)

    with open(conf_path_full, 'r') as reader:
        try:
            cf = yaml.safe_load(reader)
        except yaml.YAMLError as ex:
            print('ERR: [main]', ex)
            exit(1)
        else:
            if verbose: pp.pprint(cf)

            tpar = TPAR7K.TPAR7K(ssh_config=cf['ssh_config'])
            js_arr = tpar.getdata()

            es_eng = es.es(es_config=cf['es_config'])
            es_eng.bulk_insert(es_config=cf['es_config'], js_arr=js_arr)

    print(datetime.datetime.now() - startTime)