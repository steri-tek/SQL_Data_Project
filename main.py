#!/usr/bin/env python3

__author__ = "James Kelly"
__copyright__ = "(c) Copyright 2023, Steri-tek Corporation"
__license__ = "Closed"
__version__ = "1.0.0"
__maintainer__ = "James Kelly"
__email__ = "jamesk@steri-tek.com"
__status__ = "Development"

""" 

   This program is designed query the Mevex 110056 Database
   to obtain information on SBN's treated within a specified
   timeframe.  The tables that are queried are the SBN table,
   the QR_Master Table, and the QR Subs Table.  This is designed
   to generate a report illustrating the treatment schedule for
   the specified amount of time.
   
   NOTE: This is for the Fremont Database ONLY.
   
   SQL SBN Table:
    
    SbnID
    SBN
    Revision
    Issue
    SbnDescription
    BatchStateID
    StatusID
    CreatedTime
    Comment
    Username
    FullName
     
     
    SQL QR Master Table:
    
    QRMasterID
    Barcode
    SerialNumber
    SBN
    TBN
    EnterTreatmentSystemTime
    ExitTreatmentSystemTime
    OutfeedBarcode
    OutputStatusBitmask
    QATool
    CreatedTime
    
    SQL QR Subs Table:
    
    QRSubID
    Barcode
    LogicalUnitID
    SideNumber
    PassNumber
    FractionNumber
    EnterUbcTime
    ExitUbcTime
    TreatmentUbcSpeed
    TreatmentTimeInSec
    OutputStatusBitmask
    TreatmentSetID
    MinEnergy
    AvgEnergy
    MaxEnergy
    MinBeamI
    AvgBeamI
    MaxBeamI
    MinScanILeft
    AvgScanILeft
    MaxScanILeft
    MinScanIRight
    AvgScanIRight
    MaxScanIRight
    MinUbcSpeed
    AvgUbcSpeed
    MaxUbcSpeed
    MinDoseVariationFactor
    AvgDoseVariationFactor
    MaxDoseVariationFactor
    MinDose
    AvgDose
    MaxDose
    CreatedTime
    BEAM_I_AVE_SET
    SCAN_I_L_SET
    SCAN_I_R_SET
       
"""

from os import getenv
import logging
import pymssql
from datetime import datetime, timedelta
import sys
from fpdf import FPDF

DEBUG = True
DEBUG_VERBOSE = False

PRETTY_PRINT = False


""""

        Function: 
        Author: James Kelly
        Date: 1-May-2023

        Description:

        

"""""


def report_generator(sbn_data):

    index_a = 0
    exit_treatment_system_time = datetime.now()

    while index_a < len(sbn_data):

        print("\n\n\n")
        sbn_number = sbn_data[index_a][0]['SBN']
        print("SBN Number: ", sbn_number)
        sbn_description = sbn_data[index_a][0]['SbnDescription']
        print("SBN Description: ", sbn_description)
        sbn_created_time = sbn_data[index_a][0]['CreatedTime']
        print("SBN Created Time: ", sbn_created_time)
        # print(sbn_list[index_a])

        # Check to ensure we have a QR Master under this SBN
        number_of_qr_masters_in_sbn = len(sbn_data[index_a]) - 1

        if number_of_qr_masters_in_sbn > 0:

            # Get Treatment UBC Speed from ANY QR Sub
            # print("=========", sbn_list[index_a][1][1])
            # print("Is of type ", type(sbn_list[0][1][1][0]))
            treatment_ubc_speed = sbn_data[index_a][1][1][0]['TreatmentUbcSpeed']
            print("Treatment UBC Speed: ", treatment_ubc_speed)

            tote_length = 2.032
            release_time = (tote_length / float(treatment_ubc_speed))
            tolerance_factor = 0.4
            tolerance_factor = tolerance_factor + 1.0
            release_time = release_time * tolerance_factor

            min, sec = divmod(release_time, 1)
            sec = sec * 60
            if sec < 10:
                print("Release Time: %i:0%i" % (min, sec))
            else:
                print("Release Time: %i:%i" % (min, sec))

            index_b = 0

            while index_b < len(sbn_data[index_a]) - 1:

                # print(sbn_list[index_a][index_b + 1][0])
                # print("index a = %s     index b = %s" % (index_a, index_b))

                barcode = sbn_data[index_a][index_b + 1][0]['Barcode']
                enter_treatment_system_time = sbn_data[index_a][index_b + 1][0]['EnterTreatmentSystemTime']
                exit_treatment_system_time = sbn_data[index_a][index_b + 1][0]['ExitTreatmentSystemTime']

                if index_b == 0:

                    previous_enter_treatment_set_time = enter_treatment_system_time

                else:

                    delta_time = enter_treatment_system_time - previous_enter_treatment_set_time
                    print("Time between totes: ", delta_time)
                    previous_enter_treatment_set_time = enter_treatment_system_time

                print("Barcode: %s     Enter Treatment System Time: %s     Exit Treatment System Time: %s" %
                      (barcode, enter_treatment_system_time, exit_treatment_system_time))

                index_c = 1

                while index_c < len(sbn_data[index_a][index_b + 1]):

                    enter_ubc_time = sbn_data[index_a][index_b + 1][index_c][0]['EnterUbcTime']
                    exit_ubc_time = sbn_data[index_a][index_b + 1][index_c][0]['ExitUbcTime']
                    print(f"Enter UBC 1 Time: {enter_ubc_time}  ExitUBC 1 Time: {exit_ubc_time}")
                    index_c += 1

                index_b = index_b + 1

        index_a = index_a + 1
        index_b = 0


def main():

    """This is the main execution unit."""

    server = "10.2.10.200"
    user = "sa"
    password = "sql69jk"

    # Remove all handlers associated with the root logger object.
    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)

    # Logging Configuration and Test

    logging.basicConfig(filename='sbn_report_generator.log', level=logging.DEBUG,
                        format='%(asctime)s %(message)s',
                        datefmt='%d/%m/%Y %I:%M:%S %p')

    # Logging test
    log_string = 'Number of arguments: %s ' % len(sys.argv)
    logging.debug(log_string)
    log_string = 'Argument list: %s' % str(sys.argv)
    logging.debug(log_string)

    conn = pymssql.connect(server, user, password, "110056", tds_version="7.0")
    c1 = conn.cursor(as_dict=True)
    c1.execute('select * '
               'FROM dbo.SBNs Where CreatedTime between \'2024-02-14 06:00:00.000\' AND '
               '\'2024-02-15 06:00:00.000\' AND Username != \'System\' order by CreatedTime ASC')

    sbn_list = list()

    index_a = 0

    # Iterate through SQL Query Results

    # NOTE: sbn_list is a list of list (sbn_list[n]) with each list
    # entry containing a dictionary  representing the row returned
    # by the SQL query.

    # The first list array sbn_list[a] then is a list with each entry containing
    # an sbn entry for the Mevex SQL SBN Table. Therefore, each SBN entry of
    # sbn_list[entry][0] list element is the dictionary returned from the SBN SQL
    # query.

    for row in c1:

        log_string = '[DEBUG] row is of type', type(row)
        logging.debug(log_string)
        log_string = '[DEBUG] row = ', row
        logging.debug(log_string)
        sbn_list.append(list())
        sbn_list[index_a].append(row)
        #print(f"{sbn_list[index_a]}")
        #print(f"sbn_list[index_a] is of type {type(sbn_list[index_a])} ")
        #for element in sbn_list[index_a]:

        #    print(f"element is {element} and is of type {type(element)}")

        #print(f"The length of the sbn_list is {len(sbn_list)}")
        #print(f"The length of the sbn_list[index_a] is {len(sbn_list[index_a])}")
        #print(f"{sbn_list[index_a][0]['SBN']}")
        index_a = index_a + 1

    index_a = 0
    index_b = 1

    # Iterate through the sbn_list to get SBN Numbers which will be used
    # to form the SQL Query to get the QR Master. While sbn_list[index_a][0]
    # is the dictionary, element sbn_list[index_a][1] will be a list of
    # dictionaries for QR Master Records. There should only be one element in
    # the list

    for masters in sbn_list:

        if PRETTY_PRINT:
            print(masters)

        # log master to the DEBUG log
        logging.debug(masters)

        # QR Master Query
        sbn_string = "\'" + str(masters[0]['SBN']) + "\'"
        query_string = "select * FROM dbo.QRMasters Where SBN = " + sbn_string
        logging.debug(query_string)
        c1.execute(query_string)

        for qr_master_row in c1:

            if PRETTY_PRINT:
                print(qr_master_row)

            logging.debug(qr_master_row)

            sbn_list[index_a].append(list())
            sbn_list[index_a][index_b].append(qr_master_row)
            #print(f"[{sbn_list[index_a][index_b]}")
            index_b = index_b + 1

        index_a = index_a + 1
        index_b = 1

    # Final loop where we get QR Subs for each QR Master for each SBN and add them
    # as a list item to the parent QR Master

    index_a = 0

    while index_a < len(sbn_list):

        # In the following code, sbn_list[n] is a list of the SBN's and
        # the master records. Entry [0] is a dictionary containing the
        # SBN data while the second array element y in sbn_data[x][y],
        # is the list of master records (each one being a dictionary entry).

        # If index_b starts at 0 then the sbn dictionary is printed out too
        # if index_b starts at 1 then only the master records for that sbn
        # are listed.
        # index_c represents the QR Sub records

        index_b = 1

        while index_b < len(sbn_list[index_a]):

            qr_master_dict = sbn_list[index_a][index_b][0]
            qr_master_string = "\'" + sbn_list[index_a][index_b][0]['Barcode'] + "\'"
            query_string = "select * FROM dbo.QRsubs Where Barcode = " + qr_master_string
            c1.execute(query_string)

            index_c = 1

            for qr_subs_row in c1:

                if PRETTY_PRINT:
                    print(qr_subs_row)

                #print(f"index_c = {index_c}")
                sbn_list[index_a][index_b].append(list())
                sbn_list[index_a][index_b][index_c].append(qr_subs_row)
                #print(f"[{sbn_list[index_a][index_b][index_c]}")
                index_c = index_c + 1

            index_b = index_b + 1

        index_a = index_a + 1

    conn.close()

    report_generator(sbn_list)

    # Custom class to overwrite the header and footer methods
    class PDF(FPDF):
        def __init__(self):
            super().__init__()

        def header(self):

            self.set_font('Arial', '', 10)
            #self.cell(0, 10, 'Header', 0, 1, 'C')
            self.cell(0, 10, border=0, align='L', link=pdf.image('assets/steri-tek.png', 10, 8, 25))
            pdf.set_xy(x=10, y=20)  # or use pdf.ln(50)
            self.cell(0, 5, txt='Steri-tek Silicon Valley', align='L', border=0, ln=1)
            self.cell(0, 5, txt='48225 Lakeview Blvd', align='L', border=0, ln=1)
            self.cell(0, 5, txt='Fremont, CA 94538', align='L', border=0, ln=1)


        def footer(self):
            self.set_y(-15)
            self.set_font('Arial', '', 10)
            self.cell(0, 10, 'For Steri-tek Internal Use Only', 1, 0, 'C')
            pdf = PDF()  # Instance of custom class

    pdf = PDF()  # Instance of custom class
    pdf.add_page()
    pdf.set_font('Arial', '', 10)
    pdf.set_xy(x=10, y=40)  # or use pdf.ln(50)
    pdf.cell(w=80, h=10, txt="Report Generated Date:  09-NOV-2023 08:13:46", border=1, ln=1, align='L')
    pdf.set_xy(x=90, y=40)  # or use pdf.ln(50)
    pdf.cell(w=57, h=10, txt="Start Date: 08-NOV-2023 06:00:00", border=1, ln=1, align='L')
    pdf.set_xy(x=147, y=40)  # or use pdf.ln(50)
    pdf.cell(w=57, h=10, txt="Stop Date: 09-NOV-2023 06:00:00", border=1, ln=1, align='L')


    sbn_number_string = "SBN: 58721"
    sbn_description_string = "DEX05 - Dexcom, Inc.Job  # 47169"
    sbn_created_time_string = "Created Time: 2023-11-06 06:30:43"
    sbn_ubc_speed_string = "UBC Speed: 0.956 m/m"

    cell_x = 10
    cell_y = 60
    cell_w = len(sbn_number_string) + 15
    cell_h = 10

    pdf.set_xy(x=cell_x, y=cell_y)
    pdf.cell(w=cell_w, h=cell_h, txt=sbn_number_string, border=1, ln=1, align='C')

    cell_x = cell_x + cell_w
    pdf.set_xy(x=cell_x, y=cell_y)
    cell_w = len(sbn_description_string) + 30
    pdf.cell(w=cell_w, h=cell_h, txt=sbn_description_string, border=1, ln=1, align='C')

    cell_x = cell_x + cell_w
    pdf.set_xy(x=cell_x, y=cell_y)
    cell_w = len(sbn_created_time_string) + 30
    pdf.cell(w=cell_w, h=cell_h, txt=sbn_created_time_string, border=1, ln=1, align='C')

    cell_x = cell_x + cell_w
    pdf.set_xy(x=cell_x, y=cell_y)
    cell_w = len(sbn_ubc_speed_string) + 30
    pdf.cell(w=cell_w, h=cell_h, txt=sbn_ubc_speed_string, border=1, ln=1, align='C')

    pdf.output(f'./example.pdf', 'F')


if __name__ == '__main__':
    main()

