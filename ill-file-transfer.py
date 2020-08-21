# Processes ILL files in the OCLC staging folder
#
# Files (typically PDFs) in the OCLC staging folder have been reviewed
# by ILL staff and are ready for transfer to the OCLC remote host for
# upload to ILLiad. Once in ILLiad, the file can be retrieved by the
# requesting patron.
#
# This program simply transfers, via FTP, all PDFs in the staging folder
# to the remote OCLC host. The correpsonding log file will be annotated
# accordingly and the file will be renamed.

# Revision History
# ----------------
# 1.0 2016-12-07 FS Initial release
# 1.1 2016-12-28 FS Added logic to run script at a preset interval indefinitely
# 1.2 2016-12-29 FS Modified error handling for file archiving
#                   Modified file transfer checking logic
# 1.3 2017-04-14 FS Added error handling for files that cannot be opened
#                   Added file size to log entry
# 1.4 2017-09-05 FS Added ren (rename) function to add prefix to processed files
#                   Added error handling for potential FTP errors
#                   Added error log file write capability
# 1.5 2017-09-07 FS Fixed logic error in main loop affecting FTP connection
# 1.6 2017-09-11 FS Changed rename to replace in ren function

import glob
import sys
import os
import io
import datetime
import time
import errno
from ftplib import FTP

PDFDIR  = '//ucbfiles/LIBR/Groups/Illiad/PDF/'
ARCHDIR = '//ucbfiles/LIBR/Groups/Illiad/Archive' # DEPRECATED
FTPHOST = '206.107.44.246'
FTPUSER = 'hosted'
FTPPWD  = 'UcB5fTp*'
LOGFILE = 'ill-transfer-log.txt'
ERRORLOG = 'ill-transfer-error.txt'

# Checks if the file has already been transferred by comparing it
# to the remote host directory listing
def transferred(file, lst):
    return file in lst

# Sends the file to the remote host
def send(file, ftp):
    fo = open(file, 'rb')
    ftp.storbinary('STOR ' + os.path.basename(file), fo)
    fo.close()

# Creates a log entry
def log(file):
    dt = datetime.datetime.today().strftime('%Y-%m-%d %H:%M:%S')
    logfile = open(LOGFILE, 'a')
    logfile.write(dt + ' ' + os.path.basename(file) + ' ' + str(os.path.getsize(file)) + '\n')
    logfile.close()

# On error, write to the error log
def errorlog(msg):
	dt = datetime.datetime.today().strftime('%Y-%m-%d %H:%M:%S')
	logfile = open(ERRORLOG, 'a')
	logfile.write(dt + ' ' + msg + '\n')
	logfile.close()

# DEPRECATED
# Moves the file to the archive folder
# If the file already exists in the archive folder, it will be deleted instead
def archive(file):
    try:
        os.rename(file, os.path.normpath(ARCHDIR + '/' + os.path.basename(file)))
    except OSError as e:
        if e.errno == errno.EEXIST:
            os.remove(file)
        pass

# Replaces file by adding 'processed' to the file name, overwriting as required
# Files with this prefix will not be processed
def ren(file):
    os.replace(file, os.path.normpath(PDFDIR + 'processed-' + os.path.basename(file)))

# Main processing loop
#
# All PDFs located in the staging folder will be processed and logged
# If the file has been already uploaded, it will be skipped
# Once transferred, the file will be renamed
# The process repeats every 60 seconds
while True:
    files = glob.glob(os.path.normpath(PDFDIR + '*.pdf'))
    if len(files) != 0:
        try:
            ftp = FTP(FTPHOST)
            ftp.login(FTPUSER, FTPPWD)
            ftp.cwd('illiad/pdf')
            for f in files:
                if not f.startswith('processed', len(PDFDIR)):
                    if not transferred(f, ftp.nlst()):
                        send(f, ftp)
                        log(f)
                    ren(f)
            ftp.close()
        except:
            errorlog('Unexpected error: ' + str(sys.exc_info()[1]))
        finally:
            ftp.close()
    time.sleep(60)
