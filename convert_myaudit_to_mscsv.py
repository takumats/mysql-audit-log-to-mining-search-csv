import io
import csv
import datetime
import argparse

# Set true to skip 'rdsadmin' user activity
SKIP_RDSADMIN = True
# Set no. of steps for progress display
LINE_COUNT_STEP = 100000

# Write CSV lines for the specified log file.
def write_csv_lines_for_the_file(filename, csv_writer, session_start_time, session_end_time):
    print('Create sql list and output to the file')
    line_count = 0
    with open(filename, encoding='utf8', newline='') as f:
        for line in f:
            line_count = line_count + 1
            if (line_count % LINE_COUNT_STEP == 0):
               print('  processed:' + str(line_count))

            # PythonのCSV Readerで、シングルクォーテーションのQuotecharを使った時に
            # \' によるエスケープをうまく処理できないため '' の形式に変えてから処理
            # ここでは中間ファイルを作らないようにするため毎行でcsv.readerを作っている
            line = line.replace("\\'", "''")
            fs = io.StringIO()
            fs.write(line)
            fs.seek(0)
            csvreader = csv.reader(fs, quotechar="'")
            
            for row in csvreader:
                if row[6] != 'QUERY':
                    continue
                # skip if db is not set
                if len(row[7]) == 0:
                    continue
                # skip if SKIP_RDSADMIN is True and the user is 'rdsadmin'
                if SKIP_RDSADMIN and row[2] == 'rdsadmin':
                    continue

                a_sql_start_time = datetime.datetime.fromtimestamp(int(row[0])//1000000).strftime('%Y%m%d%H%M%S')

                # skip if no session start info
                if row[4] not in session_start_time:
                    continue

                a_sql_start_time_msec = str(int(row[0])%1000000)

                row_to_be_written = []
                row_to_be_written.append(row[1]) # Host
                row_to_be_written.append(row[7]) # Database
                row_to_be_written.append(row[4]) # SID
                row_to_be_written.append('')     # Serial
                row_to_be_written.append(session_start_time[row[4]])     # Logged In
                row_to_be_written.append(session_end_time[row[4]] if row[4] in session_end_time else '')     # Logged Out
                row_to_be_written.append(row[2]) # DB User
                row_to_be_written.append(a_sql_start_time) # SQL Start Time
                row_to_be_written.append(a_sql_start_time_msec) # SQL Start Time(Micro Sec)
                row_to_be_written.append(row[8]) # SQL Text
                row_to_be_written.append('')     # Bind Variables
                row_to_be_written.append('')     # Object
                row_to_be_written.append('')     # Elapsed Time
                row_to_be_written.append('')     # Program
                row_to_be_written.append(row[3]) # Client Information - Host

                csv_writer.writerow(row_to_be_written)

            fs.close()
    print('  processed:' + str(line_count))

# Check session information for the specified file
def create_session_list(filename):
    session_start_time = {}
    session_end_time = {}

    # Check by CONNECT/DISCONNECT
    print('Create session list by CONNECT/DISCONNECT')
    line_count = 0
    with open(filename, encoding='utf8', newline='') as f:
        csvreader = csv.reader(f)
        for row in csvreader:
            line_count = line_count + 1
            if (line_count % LINE_COUNT_STEP == 0):
               print('  processed:' + str(line_count))

            # skip if SKIP_RDSADMIN is True and the user is 'rdsadmin'
            if SKIP_RDSADMIN and row[2] == 'rdsadmin':
                continue
            session_id = row[4]
            if row[6] == 'CONNECT':
                a_session_start_time = datetime.datetime.fromtimestamp(int(row[0])//1000000).strftime('%Y%m%d%H%M%S')
                if session_id in session_start_time:
                    print('ERROR: same session id ' + session_id + ', ' + session_start_time[session_id] + ', ' + a_session_start_time)
                    break
                session_start_time[session_id] = a_session_start_time
            elif row[6] == 'DISCONNECT':
                a_session_end_time = datetime.datetime.fromtimestamp(int(row[0])//1000000).strftime('%Y%m%d%H%M%S')
                if session_id in session_end_time:
                    print('ERROR: same session id ' + session_id + ', ' + session_end_time[session_id] + ', ' + a_session_end_time)
                    break
                session_end_time[session_id] = a_session_end_time
    print('  processed:' + str(line_count))
    
    # Check by QUERY (if no CONNECT info, use first query as login and last query as logout)
    print('Create session list by QUERY')
    line_count = 0
    with open(filename, encoding='utf8', newline='') as f:
        csvreader = csv.reader(f)
        for row in csvreader:
            line_count = line_count + 1
            if (line_count % LINE_COUNT_STEP == 0):
               print('  processed:' + str(line_count))

            # skip if SKIP_RDSADMIN is True and the user is 'rdsadmin'
            if SKIP_RDSADMIN and row[2] == 'rdsadmin':
                continue
            if row[6] != 'QUERY':
                continue
            session_id = row[4]

            a_sql_start_time = datetime.datetime.fromtimestamp(int(row[0])//1000000).strftime('%Y%m%d%H%M%S')

            # skip if no session start info
            if session_id not in session_start_time:
                # use this sql start time
                session_start_time[session_id] = a_sql_start_time
            if session_id not in session_end_time:
                # use this sql start time
                session_end_time[session_id] = a_sql_start_time
            
            if a_sql_start_time < session_start_time[session_id]:
                session_start_time[session_id] = a_sql_start_time
            if session_end_time[session_id] < a_sql_start_time:
                session_end_time[session_id] = a_sql_start_time
    print('  processed:' + str(line_count))

    return session_start_time, session_end_time

def main():
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument(
        'audit_log',
        metavar='AUDIT_LOG',
        help='監査ログのファイル名'
    )
    arg_parser.add_argument(
        'output_csv_file_name',
        metavar='OUTPUT_CSV_FILE_NAME',
        help='マイニングサーチ形式CSV出力ファイル名'
    )

    # 引数取得
    args = arg_parser.parse_args()
    audit_log = args.audit_log
    output_csv_file_name = args.output_csv_file_name

    # Session Login/Logout information
    session_start_time, session_end_time = create_session_list(audit_log)
    print('no. of sessions:' + str(len(session_start_time)))

    # Create csv file
    with open(output_csv_file_name, 'w') as fo:
        writer = csv.writer(fo, quotechar='"', quoting=csv.QUOTE_NONNUMERIC)
        CSV_HEADER = ['Host','Database','SID','Serial','Logged In','Logged Out','DB User','SQL Start Time','SQL Start Time(Micro Sec)','SQL Text','Bind Variables','Object','Elapsed Time','Program','Client Information - Host']

        writer.writerow(CSV_HEADER)
        write_csv_lines_for_the_file(audit_log, writer, session_start_time, session_end_time)

if __name__ == '__main__':
    main()