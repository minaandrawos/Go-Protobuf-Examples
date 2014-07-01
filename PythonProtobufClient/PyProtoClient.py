from optparse import OptionParser
import ProtoTest_pb2
import csv
import socket

CLIENT_NAME = 'PyClient'
CLIENT_ID = 1
CLIENT_DESCRIPTION = 'This is a Python script!!'

def RetrieveMessagesValuesFromFile(filename):
    if filename=='':
        print "Error: No File Name Specified"
        return
    with open(filename ,'r') as f:
        csvreader = csv.reader(f, delimiter=',')
        header = next(csvreader, None)
        print "Headers:", header

        MessageToSend = ProtoTest_pb2.TestMessage()
        MessageToSend.clientId = CLIENT_ID
        MessageToSend.clientName = CLIENT_NAME
        MessageToSend.description = CLIENT_DESCRIPTION

        for row in csvreader:
            print row
            try:
                Item = MessageToSend.messageitems.add()
                Item.id = int(row[header.index('itemid')])
                Item.itemName = row[header.index('itemname')]
                Item.itemValue = int(row[header.index('itemvalue')])
                Item.itemType = int(row[header.index('itemType')])

            except Exception, e:
                print "Error %s occured while parsing message data from the csv file, please double check the input file" % str(e)

        print "Number of items in message to send", str(len(MessageToSend.messageitems))

        return MessageToSend.SerializeToString()

def SendProtoMessage(protomessage , dstadress):
    addresssplit = str(dstadress).split(':')
    if len(addresssplit)>2:
        print "Invalid destination address"
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect((addresssplit[0], int(addresssplit[1])))
        s.send(protomessage)
    except Exception, e:
        print "Error %s occured while attempting to send data" % str(e)
    finally:
        s.close()





if __name__ == "__main__":
    parser = OptionParser()
    parser.add_option('-d', help='specify tcp server', default='127.0.0.1:2110', action='store')
    parser.add_option('-f', help='CSV file name where the data exists', default='CSVValues.csv', action='store')
    (options, args) = parser.parse_args()
    serializedmessage = RetrieveMessagesValuesFromFile(options.f)
    SendProtoMessage(serializedmessage, options.d)




