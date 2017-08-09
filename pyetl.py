#!/usr/bin/python
from confluent_kafka import Consumer, KafkaError
import json
import re
import time
import datetime
import shutil
import gzip
import os
import sys
import random


if sys.version_info > (3,):
    try:
        import pandas as pd
        from fastparquet import write as parqwrite
        from fastparquet import ParquetFile
    except:
        print("Attempting to import pands or fastparquet failed - Parquet writer WILL fail - are you sure your container has this support??")
else:
    try:
        from pychbase import Connection, Table, Batch
    except:
        print("Attempting to import pychbase failed - MaprDB writer WILL fail - ar you sure your contianer has this support??")


# Variables - Should be setable by arguments at some point

envvars = {}
# envars['var'] = ['default', 'True/False Required', 'str/int']
# What type of destination will this instance be sending to
envvars['dest_type'] = ['', True, 'str'] # mapdb, parq, json

#Kafka/Streams
envvars['zookeepers'] = ['', False, 'str']
envvars['kafka_id'] = ['', False, 'str']
envvars['bootstrap_brokers'] = ['', False, 'str']
envvars['offset_reset'] = ['earliest', False, 'str']
envvars['group_id'] = ['', True, 'str']
envvars['topic'] = ['', True, 'str']
envvars['loop_timeout'] = ["5.0", False, 'flt']

# Field Creation - used as a basic way to create a field based on another field.
# If src is set and dest is not, the field is not created - Error occurs
# Example: src = ts, dst = ts_part, start = 0, end = 10. This would change a value like 2017-08-08T21:26:10.843768Z to 2017-08-08

envvars['derived_src'] = ['', False, 'str']  # The field to src
envvars['derived_dst'] = ['', False, 'str']  # The field to put in the dest
envvars['derived_start'] = [0, False, 'int'] # The position to start
envvars['derived_end'] = [0, False, 'int']   # The position to end
envvars['derived_req'] = [0, False, 'int']   # Fail if the addition/conversation fails

#Loop Control
envvars['rowmax'] = [50, False, 'int']
envvars['timemax'] = [60, False, 'int']
envvars['sizemax'] = [256000, False, 'int']

# Parquet Options
envvars['parq_offsets'] = [50000000, False, 'int']
envvars['parq_compress'] = ['SNAPPY', False, 'str']
envvars['parq_has_nulls'] = [1, False, 'bool']
envvars['parq_merge_file'] = [0, False, 'int']

# JSON Options
envvars['json_gz_compress'] = [0, False, 'bool'] # Not supported yet

# MapR-DB Options
envvars['maprdb_table_base'] = ['', True, 'str']
envvars['maprdb_row_key_fields'] = ['', True, 'str']
envvars['maprdb_row_key_delim'] = ['_', False, 'str']
envvars['maprdb_family_mapping'] = ['', True, 'str']
envvars['maprdb_create_table'] = [0, False, 'int']
envvars['maprdb_batch_enabled'] = [0, False, 'int']
envvars['maprdb_print_drill_view'] = [0, False, 'int']

#File Options
envvars['file_maxsize'] = [8000000, False, 'int']
envvars['file_uniq_env'] = ['HOSTNAME', False, 'str']
envvars['file_partition_field'] = ['day', False, 'str']
envvars['file_partmaxage'] = ['600', False, 'int']
envvars['file_unknownpart'] = ['unknown', False, 'str']
envvars['file_table_base'] = ['', True, 'str']
envvars['file_tmp_part_dir'] = ['.tmp', False, 'str']
envvars['file_write_live'] = [0, False, 'int']


# Bad Data Management
envvars['remove_fields_on_fail'] = [0, False, 'int'] # If Json fails to import, should we try to remove_fields based on 'REMOVE_FIELDS' 
envvars['remove_fields'] = ['', False, 'str'] # Comma Sep list of fields to try to remove if failure on JSON import
# Debug
envvars['debug'] = [0, False, 'int']
#envvars['drop_req_body_on_error'] = [1, False, 'int']


loadedenv = {}
def main():
    table_schema = {}
    cf_schema = {}
    cf_lookup = {}
    table = None

    global loadedenv
    loadedenv = loadenv(envvars)
    if loadedenv['dest_type'] != 'maprdb':
        loadedenv['tmp_part'] = loadedenv['file_table_base'] + "/" + loadedenv['file_tmp_part_dir']
        loadedenv['uniq_val'] = os.environ[loadedenv['file_uniq_env']]

    if loadedenv['debug'] == 1:
        print(json.dumps(loadedenv, sort_keys=True, indent=4, separators=(',', ': ')))


    if loadedenv['derived_src'] != '' and loadedenv['derived_dst'] == '':
        print("If adding a field, you must have a field name")
        print("derived_src %s - derived_dst: %s" % (loadedenv['derived_src'], loadedenv['derived_dst']))
        sys.exit(1)


    if loadedenv['dest_type'] == 'parq':
        if not sys.version_info > (3,):
            print("Python 2 is not supported for Parquet Writer, please use Python 3")
            sys.exit(1)
    elif loadedenv['dest_type'] == 'maprdb':
        if sys.version_info > (3,):
            print("Python 3 is not supported for maprdb load please use Python 2")
            sys.exit(1)

        table_schema, cf_schema, cf_lookup = loadmaprdbschemas()
        myview = drill_view(table_schema)
        if loadedenv['debug'] >= 1 or loadedenv['maprdb_print_drill_view'] == 1: 
            print("Drill Shell View:")
            print( myview)
        if loadedenv['maprdb_print_drill_view'] == 1:
            sys.exit(0)
        if loadedenv['debug'] >= 1:
            print("Schema provided:")
            print(table_schema)
            print("")
            print("cf_lookip:")
            print(cf_lookup)
        connection = Connection()
        try:
            table = connection.table(loadedenv['maprdb_table_base'])
        except:
            if loadedenv['maprdb_create_table'] != 1:
                print("Table not found and create table not set to 1 - Cannot proceed")
                sys.exit(1)
            else:
                print("Table not found: Creating")
                connection.create_table(loadedenv['maprdb_table_base'], cf_schema)
                try:
                    table = connection.table(loadedenv['maprdb_table_base'])
                except:
                    print("Couldn't find table, tried to create, still can't find, exiting")
                    sys.exit(1)



    if not loadedenv['dest_type'] == 'maprdb':
        if not os.path.isdir(loadedenv['tmp_part']):
            os.makedirs(loadedenv['tmp_part'])
        curfile = loadedenv['uniq_val'] + "_curfile."  + loadedenv['dest_type']


    # Get the Bootstrap brokers if it doesn't exist
    if loadedenv['bootstrap_brokers'] == "":
        if loadedenv['zookeepers'] == "":
            print("Must specify either Bootstrap servers via BOOTSTRAP_BROKERS or Zookeepers via ZOOKEEPERS")
            sys.exit(1)
        mybs = bootstrap_from_zk(loadedenv['zookeepers'], loadedenv['kafka_id'])
    else:
        if loadedenv['bootstrap_brokers'] == 'mapr':
            mybs = ''

    if loadedenv['debug'] >= 1:
        print (mybs)

    # Create Consumer group to listen on the topic specified

    c = Consumer({'bootstrap.servers': mybs, 'group.id': loadedenv['group_id'], 'default.topic.config': {'auto.offset.reset': loadedenv['offset_reset']}})
    c.subscribe([loadedenv['topic']], on_assign=print_assignment)
    # Initialize counters
    rowcnt = 0
    sizecnt = 0
    lastwrite = int(time.time()) - 1

    dataar = []
    part_ledger = {}

    # Listen for messages
    running = True
    while running:
        curtime = int(time.time())
        timedelta = curtime - lastwrite
        try:
            message = c.poll(timeout=loadedenv['loop_timeout'])
        except KeyboardInterrupt:
            print("\n\nExiting per User Request")
            c.close()
            sys.exit(0)

        if message == None:
            # No message was found but we still want to check our stuff
            pass
        elif not message.error():
            rowcnt += 1
            jmsg, errcode = returnJSONRecord(message)
            if errcode == 0:
                sizecnt += len(json.dumps(jmsg))
                dataar.append(jmsg)
        elif message.error().code() != KafkaError._PARTITION_EOF:
            print("MyError: " + message.error())
            running = False
            break


           # If our row count is over the max, our size is over the max, or time delta is over the max, write the group .
        if (rowcnt >= loadedenv['rowmax'] or timedelta >= loadedenv['timemax'] or sizecnt >= loadedenv['sizemax']) and len(dataar) > 0:
            if loadedenv['dest_type'] != 'maprdb':
                part_ledger = writeFile(dataar, part_ledger, curfile, curtime, rowcnt, sizecnt, timedelta)
                part_ledger = dumpPart(part_ledger, curtime)
            else:
                writeMapRDB(dataar, table, cf_lookup, rowcnt, sizecnt, timedelta)
            rowcnt = 0
            sizecnt = 0
            lastwrite = curtime
            dataar = []

    c.close()

def writeMapRDB(dataar, table, cf_lookup, rowcnt, sizecnt, timedelta):
    if loadedenv['maprdb_batch_enabled'] == 1:
        batch = table.batch()
        for r in dataar:
            batch.put(db_rowkey(r), db_row(r, cf_lookup))
        batch_errors = batch.send()
        if batch_errors == 0:
            if loadedenv['debug'] >= 1:
                print("%s Write batch to %s at %s records - Size: %s - Seconds since last write: %s - NO ERRORS" % (datetime.datetime.now(), loadedenv['maprdb_table_base'], rowcnt, sizecnt, timedelta))
        else:
            print("Multiple errors on write - Errors: %s" % batch_errors)
            sys.exit(1)
    else:
        bcnt = 0
        for r in dataar:
            bcnt += 1
            try:
                table.put(db_rowkey(r), db_row(r, cf_lookup))
            except:
                print("Failed on record with key: %s" % db_rowkey(r))
                print(db_row(r, cf_lookup))
                sys.exit(1)
        if loadedenv['debug'] >= 1:
            print("Pushed: %s rows" % rowcnt)



def dumpPart(pledger, curtime):
    removekeys = []
    for x in pledger.keys():
        l = pledger[x][0]
        s = pledger[x][1]
        f = pledger[x][2]
        fw = pledger[x][3]
        base_dir = loadedenv['file_table_base'] + '/' + x
        if not os.path.isdir(base_dir):
            try:
                os.makedirs(base_dir)
            except:
                print("Partition Create failed, it may have been already created for %s" % (base_dir))
        if s > loadedenv['file_maxsize'] or (curtime - fw) > loadedenv['file_partmaxage']:
            new_file_name = loadedenv['uniq_val'] + "_" + str(curtime) + "." + loadedenv['dest_type']
            new_file = base_dir + "/" + new_file_name

            if loadedenv['debug'] >= 1:
                outreason = ""
                if s > loadedenv['file_maxsize']:
                    outreason = "Max Size"
                else:
                    outreason = "Max Age"
                print("%s %s reached - Size: %s - Age: %s - Writing to %s" % (datetime.datetime.now(), outreason, s, curtime - l, new_file))

            if loadedenv['dest_type'] == 'json':
                if loadedenv['json_gz_compress'] == 1:
                    if loadedenv['debug'] >= 1:
                        print("Compressing json files")
                    with open(f, 'rb') as f_in:
                        with gzip.open(f + ".gz", 'wb') as f_out:
                            shutil.copyfileobj(f_in, f_out)
                    os.remove(f)
                    f = f + ".gz"
                    new_file = new_file + ".gz"
            shutil.move(f, new_file)
            removekeys.append(x)

                # If merge_file is 1 then we read in the whole parquet file and output it in one go to eliminate all the row groups from appending
            if loadedenv['dest_type'] == 'parq':
                if loadedenv['parq_merge_file'] == 1:
                    if loadedenv['debug'] >= 1:
                       print("%s Merging parqfile into to new parq file" % (datetime.datetime.now()))
                    inparq = ParquetFile(new_file)
                    inparqdf = inparq.to_pandas()
                    tmp_file = loadedenv['tmp_part'] + "/" + new_file_name
                    parqwrite(tmp_file, inparqdf, compression=loadedenv['parq_compress'], row_group_offsets=loadedenv['parq_offsets'], has_nulls=loadedenv['parq_has_nulls'])
                    shutil.move(tmp_file, new_file)
                    inparq = None
                    inparqdf = None
    for y in removekeys:
        del pledger[y]
    return pledger


def writeFile(dataar, pledger, curfile, curtime, rowcnt, sizecnt, timedelta):
    parts = []
    if loadedenv['dest_type'] == 'parq':
        parqdf = pd.DataFrame.from_records([l for l in dataar])
        parts = parqdf[loadedenv['file_partition_field']].unique()
        if len(parts) == 0:
            print("Error: Records without Partition field - Using default Partition of %s" % loadedenv['file_unknownpart']) # Need to do better job here
            parts.append(loadedenv['file_unknownpart'])
    else:
        parts = []
        for x in dataar:
            try:
                p = x[loadedenv['file_partition_field']]
            except:
                p = loadedenv['file_unknownpart']
            if not p in parts:
                parts.append(p)



    if loadedenv['debug'] >= 1:

        print("%s Write Data batch to %s at %s records - Size: %s - Seconds since last write: %s - Partitions in this batch: %s" % (datetime.datetime.now(), curfile, rowcnt, sizecnt, timedelta, parts))

    for part in parts:
        if loadedenv['dest_type'] == 'parq':
            partdf =  parqdf[parqdf[loadedenv['file_partition_field']] == part]
        else:
            partar = []
            for x in dataar:
                try:
                    curpart = x[loadedenv['file_partition_field']]
                except:
                    curpart = loadedenv['file_unknownpart']
                if curpart == part:
                    partar.append(x)

        if loadedenv['file_write_live'] == 1:
            base_dir = loadedenv['file_table_base'] + "/" + part
        else:
            base_dir = loadedenv['file_table_base'] + "/" + loadedenv['file_tmp_part_dir'] + "/" + part
 


        final_file = base_dir + "/" + curfile
        if not os.path.isdir(base_dir):
            try:
                os.makedirs(base_dir)
            except:
                print("Partition Create failed, it may have been already created for %s" % (base_dir))

        if loadedenv['debug'] >= 1:
            print("----- Writing partition %s to %s" % (part, final_file))

        if loadedenv['dest_type'] == 'parq':
            if not os.path.exists(final_file):
                parqwrite(final_file, partdf, compression=loadedenv['parq_compress'], row_group_offsets=loadedenv['parq_offsets'], has_nulls=loadedenv['parq_has_nulls'])
            else:
                parqwrite(final_file, partdf, compression=loadedenv['parq_compress'], row_group_offsets=loadedenv['parq_offsets'], has_nulls=loadedenv['parq_has_nulls'], append=True)
            partdf = pd.DataFrame()
        else:
            fout = open(final_file, 'a')
            for x in partar:
                fout.write(json.dumps(x) + "\n")
            fout.close()
            partar = []

        cursize =  os.path.getsize(final_file)
        if part in pledger:
            firstwrite = pledger[part][3]
        else:
            firstwrite = curtime

        ledger = [curtime, cursize, final_file, firstwrite]
        pledger[part] = ledger
    return pledger





# Take a Kafka Messaage object (m) and try to make a json record out of it
def returnJSONRecord(m):
    retval = {}
    failedjson = 0
    # This is  message let's add it to our queue
    try:
        # This may not be the best way to approach this.
        retval = m.value().decode('ascii', errors='replace')
    except:
        print(m.value())
        failedjson = 3
           # Only write if we have a message
    if retval != "" and failedjson == 0:
        try:
            retval = json.loads(retval)
        except:
            failedjson = 1
            if loadedenv['remove_fields_on_fail'] == 1:
                print("%s JSON Error likely due to binary in request - per config remove_field_on_fail - we are removing the the following fields and trying again" % (datetime.datetime.now()))
                while failedjson == 1:
                    repval = m.value()
                    for f in loadedenv['remove_fields'].split(","):
                        print("Trying to remove: %s" % f)
                        if sys.version_info > (3,):
                            repval = re.sub(b'"' + f.encode() + b'":".+?","', b'"' + f.encode() + b'":"","', repval)
                        else:
                            repval = re.sub('"' + f + '":".+?","', '"' + f + '":"","', repval)

                        try:
                            retval = json.loads(repval.decode("ascii", errors='ignore'))
                            failedjson = 0
                            break
                        except:
                            print("Still could not force into json even after dropping %s" % f)
                    if failedjson == 1:
                        if loadedenv['debug'] == 1:
                            print(repval.decode("ascii", errors='ignore'))
                        failedjson = 2

        if loadedenv['debug'] >= 1 and failedjson >= 1:
            printJSONFail(m, retval)


    if loadedenv['derived_src'] != "":
        try:
            s = loadedenv['derived_start']
            e = loadedenv['derived_end']
            srcval = retval[loadedenv['derived_src']]
            if e != 0:
                retval[loadedenv['derived_dst']] = srcval[s:e]
            else:
                retval[loadedenv['derived_dst']] = srcval[s:]
        except:
            print("Error converting field %s" % loadedenv['derived_src'])
            if loadedenv['derived_req'] != 0:
                print("Exiting due to derived_req being set")
                sys.exit(1)


    return retval, failedjson


def printJSONFail(m, val):
    print ("JSON Error - Debug - Attempting to print")
    print("Raw form kafka:")
    try:
        print(m.value())
    except:
        print("Raw message failed to print")
    print("Ascii Decoded (Sent to json.dumps):")
    try:
        print(val)
    except:
        print("Ascii dump message failed to print")



def drill_view(tbl):
    tbl_name = loadedenv['maprdb_table_base']

    out = "CREATE OR REPLACE VIEW MYVIEW_OF_DATA as \n"
    out = out + "select \n"
    out = out + "CONVERT_FROM(`row_key`, 'UTF8') AS `tbl_row_key`,\n"
    for cf in tbl.iterkeys():
        for c in tbl[cf]:
            out = out + "CONVERT_FROM(t.`%s`.`%s`, 'UTF8') as `%s`, \n" % (cf, c, c)

    out = out[:-3] + "\n"
    out = out + "FROM `%s` t\n" % tbl_name
    return out

def db_rowkey(jrow):
    out = ""
    for x in loadedenv['maprdb_row_key_fields'].split(","):
        v = ''
        # When we don't have a lot of variance in our key generation, we can add a RANDOMROWKEYVAL to the row key
        if x == "RANDOMROWKEYVAL":
            v = str(random.randint(1,100000000))
        else:
            if jrow[x] == None:
                v = ''
            else:
                try:
                    v = str(jrow[x])
                except:
                    print(jrow)
                    sys.exit(1)

        if out == "":
            out = v
        else:
            out = out + loadedenv['maprdb_row_key_delim'] + v
    return out


def db_row(jrow, cfl):
    out ={}
    for r in jrow:
        v = ''
        if jrow[r] == None:
            v = ''
        else:
            try:
                v = str(jrow[r])
            except:
                try:
                    v = jrow[r].encode('ascii', errors='ignore').decode()
                except:
                    print("Field: %s" % r)
                    print(jrow)
        out[cfl[r] + ":" + r] = v
    return out

def loadmaprdbschemas():

    table_schema = {}
    cf_schema = {}
    cf_lookup = {}
    for x in loadedenv['maprdb_family_mapping'].split(";"):
        o = x.split(":")
        table_schema[o[0]] = o[1].split(",")
        cf_schema[o[0]] = {}
    for x in table_schema.iterkeys():
        for c in table_schema[x]:
            cf_lookup[c] = x
    return table_schema, cf_schema, cf_lookup




def loadenv(evars):
    print("Loading Environment Variables")
    lenv = {}
    val = None
    for e in evars:
        try:
            val = os.environ[e.upper()]
        except:
            if evars[e][1] == True:
                if e == 'dest_type':
                    print("Variables DEST_TYPE not found, this variable MUST be provided - exiting")
                    sys.exit(1)
                    val = None
            else:
                print("ENV Variable %s not found, but not required, using default of '%s'" % (e.upper(), evars[e][0]))
                val = evars[e][0]
        if evars[e][2] == 'int':
            val = int(val)
        if evars[e][2] == 'flt':
            val = float(val)
        if evars[e][2] == 'bool':
            val = bool(val)
        if val != None:
            lenv[e] = val
    d = lenv['dest_type']
    if d != "maprdb":
        other = "file"
    else:
        other = "notfile"
    for e in evars:
        if evars[e][1] == True:
            if not e in lenv and e.find(d) != 0 and e.find(other) != 0:
                print("ENV Variable %s is required and not provided - Exiting" % (e.upper()))
                sys.exit(1)

    return lenv


# Get our bootstrap string from zookeepers if provided
def bootstrap_from_zk(ZKs, kafka_id):
    from kazoo.client import KazooClient
    zk = KazooClient(hosts=ZKs,read_only=True)
    zk.start()

    brokers = zk.get_children('/%s/brokers/ids' % kafka_id)
    BSs = ""
    for x in brokers:
        res = zk.get('/%s/brokers/ids/%s' % (kafka_id, x))
        dj = json.loads(res[0].decode('utf-8'))
        srv = "%s:%s" % (dj['host'], dj['port'])
        if BSs == "":
            BSs = srv
        else:
            BSs = BSs + "," + srv

    zk.stop()

    zk = None
    return BSs

def print_assignment(consumer, partitions):
        if loadedenv['debug'] >= 1:
            print('Assignment of group to partitions %s' % partitions)

if __name__ == "__main__":
    main()
