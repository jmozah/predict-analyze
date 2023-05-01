import json
from pathlib import Path
import argparse
import sqlite3


# Load predictor and access list tracer results into a postgresql database block by block
def match_accounts(p_accounts, t_accounts):
    i = 0
    for account in p_accounts:
        if account in t_accounts:
            i += 1
    return i


def match_slots(p_slots, t_slots):
    i = 0
    for slot in p_slots:
        if slot in t_slots:
            i += 1
    return i


# convert ac_tracer2 acl to predict_al acl record
def summarize_access_list2(record):
    acl = record['acl']
    acl2 = {}
    acl_set = {}
    for address, funcs in acl.items():
        address_slots = set()
        if address in acl_set:
            address_slots = acl_set[address]

        if len(funcs) > 0:
            for func, func_acl in funcs.items():
                acl2[address + "-" + func] = func_acl

                for address2, slots in func_acl.items():
                    if "s" == address2:
                        if len(slots) > 0:
                            for slot in slots:
                                address_slots.add(slot)
                    elif address2 not in acl_set:
                        acl_set[address2] = set()

        acl_set[address] = address_slots

    a = []
    s = []
    ts = 0
    ta = len(acl_set)
    for address, slots in acl_set.items():
        a.append(address)
        if len(slots) > 0:
            keys = []
            for k in slots:
                keys.append(k)

            slots2 = {'address': address, 'storageKeys': keys}
            s.append(slots2)
            ts = ts + len(keys)

    rd = {
        'a': a
    }

    if len(s) > 0:
        rd['s'] = s

    record['ta'] = ta
    record['ts'] = ts
    record['rd'] = [rd]

    record['acl'] = acl2
    return record


def parse_block(conn, predictor_prefix, tracer_prefix, block_num, dry=0):
    postfix = "-" + str(block_num) + ".json"
    predictor_file = predictor_prefix + postfix
    tracer_file = tracer_prefix + postfix

    p_sql = "INSERT INTO predict(hash, block, total_touches,total_rounds, total_accounts, " \
            "total_slots, round_batches, accounts, slots, stat_time,matched_accounts, matched_slots,ratio_accounts," \
            "ratio_slots ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
    t_sql = "INSERT INTO trace(hash, block, type, status, jumpis, total_touches, total_accounts, " \
            "total_slots,accounts, slots, stat_time, acl) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"

    # Usually happens when the block is an empty block
    if not Path(predictor_file).is_file():
        raise Exception("Predictor result file {0} doesn't exist ".format(predictor_file))
    if not Path(tracer_file).is_file():
        raise Exception("Tracer result file {0} doesn't exist ".format(tracer_file))

    cur = None
    try:
        tx_hash = None
        if dry == 0:
            cur = conn.cursor()
        with open(predictor_file) as fp_predictor, open(tracer_file) as fp_tracer:
            predictor_results = json.load(fp_predictor)
            tracer_results = json.load(fp_tracer)
            if len(predictor_results) != len(tracer_results):
                raise Exception("Lengths of predictor or tracer results of block {} are not equal")

            for i in range(0, len(predictor_results)):
                p_result = predictor_results[i]
                t_result = tracer_results[i]

                if p_result['h'] != t_result['h']:
                    raise Exception("The {}th tx of block {} in predictor and tracer results are different")

                tx_hash = p_result['h']

                acl = {}
                if 'rd' not in t_result:
                    if 'acl' in t_result:
                        t_result = summarize_access_list2(t_result)
                        acl = t_result['acl']
                    else:
                        continue

                status = 1
                if 'status' in t_result:
                    status = t_result['status']
                    if status == 0:
                        continue

                p_accounts = []
                p_slots = []
                for row in p_result['rd']:
                    accounts = row.get('a')
                    if accounts:
                        for account in accounts:
                            p_accounts.append(account)
                    slots = row.get('s')
                    if slots:
                        storageKeys = slots[0].get('storageKeys')
                        if storageKeys:
                            for storageKey in storageKeys:
                                p_slots.append(storageKey)
                p_accounts = list(set(p_accounts))
                p_slots = list(set(p_slots))
                # p_accounts = p_result['rd'][0].get('a', [])
                # p_slots = p_result['rd'][0].get('s', [])

                t_accounts = []
                t_slots = []
                for row in t_result['rd']:
                    accounts = row.get('a')
                    if accounts:
                        for account in accounts:
                            t_accounts.append(account)
                    slots = row.get('s')
                    if slots:
                        storageKeys = slots[0].get('storageKeys')
                        if storageKeys:
                            for storageKey in storageKeys:
                                t_slots.append(storageKey)
                t_accounts = list(set(t_accounts))
                t_slots = list(set(t_slots))

                # t_accounts = t_result['rd'][0].get('a', [])
                # t_slots = t_result['rd'][0].get('s', [])

                ratio_accounts = 0
                ratio_slots = 0
                if t_result['type'] == 0:
                    matched_accounts = 2
                    matched_slots = 0
                else:
                    matched_accounts = match_accounts(p_accounts, t_accounts)
                    if matched_accounts > 0:
                        ratio_accounts = matched_accounts / len(t_accounts)

                    matched_slots = match_slots(p_slots, t_slots)
                    if matched_slots > 0:
                        ratio_slots = matched_slots / len(t_slots)

                if t_result['st'][-1] == 's':
                    last_two_chars = t_result['st'][-2:]
                    if last_two_chars == 'ns':
                        t_stat_time = int(t_result['st'][:-2])
                    elif last_two_chars == 'ms':
                        t_stat_time = int(float(t_result['st'][:-2]) * 1000000)
                    elif last_two_chars == 'µs':
                        t_stat_time = int(float(t_result['st'][:-2]) * 1000)
                    else:
                        t_stat_time = int(float(t_result['st'][:-1]) * 1000000000)
                else:
                    raise Exception('Failed to parse trace time {0}:{1} {2}'.format(block_num, tx_hash, t_result['st']))

                if dry == 0:
                    data = (tx_hash, block_num, p_result['tt'], p_result['tr'], p_result['ta'], p_result['ts'],
                        json.dumps(p_result['rb']), json.dumps(p_accounts), json.dumps(p_slots),
                        p_result['st'], matched_accounts, matched_slots, ratio_accounts, ratio_slots)
                    cur.execute("INSERT INTO predict VALUES (?, ?, ?, ?, ?,?, ?, ?, ?, ?, ?, ?, ?, ?)", data)
                    # cur.execute(p_sql, (
                    #     tx_hash, block_num, p_result['tt'], p_result['tr'], p_result['ta'], p_result['ts'],
                    #     json.dumps(p_result['rb']), json.dumps(p_accounts), json.dumps(p_slots),
                    #     p_result['st'], matched_accounts, matched_slots, ratio_accounts, ratio_slots))
                    #

                    data = (tx_hash, block_num, t_result['type'], status, t_result['jumpis'], t_result['tt'],
                         t_result['ta'], t_result['ts'], json.dumps(t_accounts), json.dumps(t_slots), t_stat_time,
                         json.dumps(acl))
                    cur.execute("INSERT INTO trace VALUES (?, ?, ?, ?, ?,?, ?, ?, ?, ?, ?, ?)", data)
                    # cur.execute(t_sql, (
                    #     tx_hash, block_num, t_result['type'], status, t_result['jumpis'], t_result['tt'],
                    #     t_result['ta'], t_result['ts'], json.dumps(t_accounts), json.dumps(t_slots), t_stat_time,
                    #     json.dumps(acl)))

                    conn.commit()
            return len(predictor_results)
    except Exception as e:
        raise Exception('Failed to parse block {0}:{1} {2}'.format(block_num, tx_hash, e))
    finally:
        if cur is not None:
            cur.close()


def import_data(conn, predictor_prefix, tracer_prefix, begin_block, end_block, dry=0):
    for block_num in range(begin_block, end_block):
        try:
            parse_block(conn, predictor_prefix, tracer_prefix, block_num, dry)
            print("block {} parsed".format(block_num))
        except Exception as e:
            print("failed to parse block {}: {}".format(block_num, e))


def create_tables(conn):
    # Create a cursor object
    cur = conn.cursor()

    # Read the SQL file
    sql_file = open('db.sql')
    sql_as_string = sql_file.read()

    # Execute the SQL commands
    cur.executescript(sql_as_string)

    # Save (commit) the changes
    conn.commit()
    cur.close()

def main():
    arg_parser = argparse.ArgumentParser(
        description='Import predictor and access list tracer results into database block by block')
    arg_parser.add_argument('--db', '-d', help='database ini config file name', default='database.ini')
    arg_parser.add_argument('--predictor', '-p', help='predictor result file prefix with path', required=True)
    arg_parser.add_argument('--tracer', '-t', help='tracer result file prefix with path', required=True)
    arg_parser.add_argument('--begin', '-b', help='begin block number ( including )', required=True)
    arg_parser.add_argument('--end', '-e', help='end block number ( excluded )', required=True)
    arg_parser.add_argument('--dry', '-D', help='dry run, only parse data but not touch database, default is 0',
                            type=int, default=0)

    args = arg_parser.parse_args()

    conn = None
    try:
        conn = sqlite3.connect('testdb.sqlite')
        create_tables(conn)
        import_data(conn, args.predictor, args.tracer, int(args.begin), int(args.end), args.dry)
    except (Exception, sqlite3.Error) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


if __name__ == '__main__':
    main()
