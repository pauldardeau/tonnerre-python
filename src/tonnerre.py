# Copyright Paul Dardeau, SwampBits LLC 2014
# BSD License

from chaudiere import IniReader
from chaudiere import KeyValuePairs
from chaudiere import Logger
from chaudiere import ServiceInfo
from chaudiere import Socket
from chaudiere import StringTokenizer
from chaudiere import StrUtils

#******************************************************************************
#******************************************************************************

class Messaging:
   
   KEY_SERVICES = "services"
   KEY_HOST     = "host"
   KEY_PORT     = "port"
   
   messagingInstance = None

   '''
   * Initializes the messaging system by reading the specified INI configuration file
   * @param configFilePath the path to the INI configuration file
   * @throws Exception
   '''
   @staticmethod
   def initialize(configFilePath):
      Logger.debug("Messaging.initialize: reading configuration file")
      reader = IniReader(configFilePath)
      
      if reader.hasSection(Messaging.KEY_SERVICES):
         kvpServices = KeyValuePairs()
         if reader.readSection(Messaging.KEY_SERVICES, kvpServices):
            keys = kvpServices.getKeys()
            servicesRegistered = 0
         
            messaging = Messaging()
         
            for key in keys:
               serviceName = key
               sectionName = kvpServices.getValue(serviceName)
            
               kvp = KeyValuePairs()
               if reader.readSection(sectionName, kvp):
                  if kvp.hasKey(Messaging.KEY_HOST) and kvp.hasKey(Messaging.KEY_PORT):
                     host = kvp.getValue(Messaging.KEY_HOST)
                     portAsString = kvp.getValue(Messaging.KEY_PORT)
                     portValue = int(portAsString)
                     serviceInfo = ServiceInfo(serviceName, host, portValue)
                     messaging.registerService(serviceName, serviceInfo)
                     servicesRegistered += 1
         
            if servicesRegistered > 0:
               Messaging.setMessaging(messaging)
               Logger.info("Messaging initialized")
            else:
               Logger.debug("Messaging.initialize: no services registered")
               raise Exception("no services registered")

   '''
   * Retrieves the Messaging instance
   * @return the Messaging instance (singleton), or None
   '''
   @staticmethod
   def getMessaging():
      return Messaging.messagingInstance

   '''
   * Sets the Messaging instance (singleton) to use after successfully reading configuration
   * @param messaging the new Messaging instance
   '''
   @staticmethod
   def setMessaging(messaging):
      Messaging.messagingInstance = messaging

   '''
   * Determines if the messaging system has been initialized
   * @return boolean indicating if messaging has been initialized
   '''
   @staticmethod
   def isInitialized():
      return None != Messaging.getMessaging()

   '''
   * Constructs a new Messaging instance
   '''
   def __init__(self):
      self.mapServices = {}
      
   '''
   * Registers a service with the messaging system
   * @param serviceName the name of the service being registered
   * @param serviceInfo the data (ServiceInfo instance) associated with the service
   * @see ServiceInfo()
   '''
   def registerService(self, serviceName, serviceInfo):
      self.mapServices[serviceName] = serviceInfo

   '''
   * Determines if the specified service is registered
   * @param serviceName the name of the service to check
   * @return boolean indicating if the service is registered
   '''
   def isServiceRegistered(self, serviceName):
      return self.mapServices.has_key(serviceName)

   '''
   * Retrieves configuration data for the specified service
   * @param serviceName the name of the service whose data is requested
   * @return the ServiceInfo instance containing data for the service
   * @see ServiceInfo()
   '''
   def getInfoForService(self, serviceName):
      return self.mapServices[serviceName]
   
#******************************************************************************
#******************************************************************************

class MessageType:
   Unknown, KeyValues, Text = range(3)

#******************************************************************************
#******************************************************************************

class Message:
    
   MAX_SEGMENT_LENGTH       = 32767
   NUM_CHARS_HEADER_LENGTH  = 10

   DELIMITER_KEY_VALUE      = "="
   DELIMITER_PAIR           = ";"
   EMPTY_STRING             = ""
   KEY_ONE_WAY              = "1way"
   KEY_PAYLOAD_LENGTH       = "payload_length"
   KEY_PAYLOAD_TYPE         = "payload_type"
   KEY_REQUEST_NAME         = "request"
   VALUE_PAYLOAD_KVP        = "kvp"
   VALUE_PAYLOAD_TEXT       = "text"
   VALUE_PAYLOAD_UNKNOWN    = "unknown"
   VALUE_TRUE               = "true"


   '''
    * Reconstructs a message by reading from a socket
    * @param socketConnection the socket to read from
    * @return a new Message object instance constructed by reading data from socket
    * @see Socket()
   '''
   @staticmethod
   def reconstruct(socketConnection):
      if (socketConnection != None) and socketConnection.isOpen():
         message = Message()
         if message.reconstitute(socketConnection):
            return message
      
      return None
   
   '''
    * Constructs a message in anticipation of sending it
    * @param requestName the name of the message request
    * @param messageType the type of the message
   '''
   def __init__(self, requestName=None, messageType=MessageType.Unknown):
      self.messageType = messageType
      self.isOneWay = 0
      self.kvpHeaders = KeyValuePairs()
      if requestName is not None:
         self.kvpHeaders.addPair(Message.KEY_REQUEST_NAME, requestName)
   
   '''
    * Sends a message to the specified service and disregards any response that the
    * server handler might generate.
    * @param serviceName the name of the service destination
    * @return boolean indicating if message was successfully delivered
   '''
   def send(self, serviceName):
      if self.messageType == MessageType.Unknown:
         Logger.error("Message.send: unable to send message, no message type set")
         return 0

      socketConnection = Message.socketForService(serviceName)
   
      if socketConnection is not None:
         self.isOneWay = 1
      
         if socketConnection.write(self.toString()):
            return 1
         else:
            # unable to write to socket
            Logger.error("Message.send: unable to write to socket")
      else:
         # unable to connect to service
         Logger.error("Message.send: unable to connect to service")
   
      return 0
   
   '''
    * Sends a message and retrieves the message response (synchronous call)
    * @param serviceName the name of the service destination
    * @param responseMessage the message object instance to populate with the response
    * @return boolean indicating if the message was successfully delivered and a response received
   '''
   def send(self, serviceName, responseMessage):
      if self.messageType == MessageType.Unknown:
         Logger.error("Message.send: unable to send message, no message type set")
         return 0
   
      socketConnection = Message.socketForService(serviceName)
   
      if socketConnection is not None:
         #if Logger.isLogging(Logger.LogLevel.Verbose):
         payload = self.toString()
         Logger.verbose("Message.send: payload: '" + payload + "'")
      
         if socketConnection.write(payload):
            return responseMessage.reconstitute(socketConnection)
         else:
            # unable to write to socket
            Logger.error("Message.send: unable to write to socket")
      else:
         # unable to connect to service
         Logger.error("Message.send: unable to connect to service")

      return 0
   
   '''
    * Reconstitute a message by reading message state data from a socket (used internally)
    * @param socketConnection the socket from which to read message state data
    * @return boolean indicating whether the message was successfully reconstituted
    * @see Socket()
   '''
   def reconstitute(self, socketConnection):
      if socketConnection is not None:
         if self.kvpHeaders is None:
            self.kvpHeaders = KeyValuePairs()
         
         headerLengthPrefix = socketConnection.readSocket(Message.NUM_CHARS_HEADER_LENGTH)
      
         if headerLengthPrefix is not None and len(headerLengthPrefix) == Message.NUM_CHARS_HEADER_LENGTH:
         
            headerLengthPrefix = StrUtils.stripTrailing(headerLengthPrefix, ' ')
            Logger.verbose("Message.reconstitute: headerLengthPrefix read: '" + headerLengthPrefix + "'")
            headerLength = int(headerLengthPrefix)
         
            if headerLength > 0:
               headersAsString = socketConnection.readSocket(headerLength)
               
               if headersAsString is None or len(headersAsString) != headerLength:
                  Logger.error("Message.reconstitute: reading socket for header failed")
                  return 0
            
               if len(headersAsString) > 0:
                  Logger.verbose("Message.reconstitute: headersAsString: '" + headersAsString + "'")
                  
                  if self.fromString(headersAsString, self.kvpHeaders):
                     if self.kvpHeaders.hasKey(Message.KEY_PAYLOAD_TYPE):
                        valuePayloadType = self.kvpHeaders.getValue(Message.KEY_PAYLOAD_TYPE)
                     
                        if valuePayloadType == Message.VALUE_PAYLOAD_TEXT:
                           self.messageType = MessageType.Text
                        elif valuePayloadType == Message.VALUE_PAYLOAD_KVP:
                           self.messageType = MessageType.KeyValues
                  
                     if self.messageType == MessageType.Unknown:
                        Logger.error("Message.reconstitute: unable to identify message type from header")
                        return 0
                  
                     if self.kvpHeaders.hasKey(Message.KEY_PAYLOAD_LENGTH):
                        valuePayloadLength = self.kvpHeaders.getValue(Message.KEY_PAYLOAD_LENGTH)
                     
                        if len(valuePayloadLength) > 0:
                           payloadLength = int(valuePayloadLength)
                        
                           if (payloadLength > 0) and (payloadLength <= Message.MAX_SEGMENT_LENGTH):
                              payloadAsString = socketConnection.readSocket(payloadLength)
                              if payloadAsString is None or len(payloadAsString) != payloadLength:
                                 Logger.error("Message.reconstitute: reading socket for payload failed")
                                 return 0
                           
                              if len(payloadAsString) > 0:
                                 if self.messageType == MessageType.Text:
                                    self.textPayload = payloadAsString
                                 elif self.messageType == MessageType.KeyValues:
                                    self.kvpPayload = KeyValuePairs()
                                    self.fromString(payloadAsString, self.kvpPayload)
                  
                     if self.kvpHeaders.hasKey(Message.KEY_ONE_WAY):
                        valueOneWay = self.kvpHeaders.getValue(Message.KEY_ONE_WAY)
                        if valueOneWay == Message.VALUE_TRUE:
                           # mark it as being a 1-way message
                           self.isOneWay = 1
                  
                     return 1
                  else:
                     # unable to parse header
                     Logger.error("Message.reconstitute: unable to parse header")
               else:
                  # unable to read header
                  Logger.error("Message.reconstitute: unable to read header")
            else:
               # header length is empty
               Logger.error("Message.reconstitute: header length is empty")
         else:
            # socket read failed
            Logger.error("Message.reconstitute: socket read failed")
      else:
         # no socket given
         Logger.error("Message.reconstitute: no socket given to reconstitute")

      return 0
   
   '''
    * Sets the type of the message
    * @param messageType the type of the message
   '''
   def setType(self, messageType):
      self.messageType = messageType
   
   '''
    * Retrieves the type of the message
    * @return the message type
   '''
   def getType(self):
      return self.messageType
   
   '''
    * Retrieves the name of the message request
    * @return the name of the message request
   '''
   def getRequestName(self):
      if (self.kvpHeaders is not None) and self.kvpHeaders.hasKey(Message.KEY_REQUEST_NAME):
         return self.kvpHeaders.getValue(Message.KEY_REQUEST_NAME)
      else:
         return Message.EMPTY_STRING
   
   '''
    * Retrieves the key/values payload associated with the message
    * @return reference to the key/values message payload
    * @see KeyValuePairs()
   '''
   def getKeyValuesPayload(self):
      return self.kvpPayload
   
   '''
    * Retrieves the textual payload associated with the message
    * @return reference to the textual message payload
   '''
   def getTextPayload(self):
      return self.textPayload
   
   '''
    * Sets the key/values payload associated with the message
    * @param kvp the new key/values payload
    * @see KeyValuePairs()
   '''
   def setKeyValuesPayload(self, kvp):
      self.kvpPayload = kvp
   
   '''
    * Sets the textual payload associated with the message
    * @param text the new textual payload
   '''
   def setTextPayload(self, text):
      self.textPayload = text

   '''
    * Retrieves the service name from a reconstituted message (used internally)
    * @return the name of the service
   '''
   def getServiceName(self):
      return self.serviceName
   
   '''
    * Flatten the message state to a string so that it can be sent over network connection (used internally)
    * @return string representation of message state ready to be sent over network
   '''
   #@Override
   def toString(self):
      kvpHeaders = self.kvpHeaders  #KeyValuePairs(self.kvpHeaders)
      payload = ""
   
      if self.messageType == MessageType.Text:
         kvpHeaders.addPair(Message.KEY_PAYLOAD_TYPE, Message.VALUE_PAYLOAD_TEXT)
         payload = self.textPayload
      elif self.messageType == MessageType.KeyValues:
         kvpHeaders.addPair(Message.KEY_PAYLOAD_TYPE, Message.VALUE_PAYLOAD_KVP)
         payload = self.kvpToString(self.kvpPayload)
      else:
         kvpHeaders.addPair(Message.KEY_PAYLOAD_TYPE, Message.VALUE_PAYLOAD_UNKNOWN)
   
      if self.isOneWay:
         kvpHeaders.addPair(Message.KEY_ONE_WAY, Message.VALUE_TRUE)
   
      if self.kvpHeaders.hasKey(Message.KEY_REQUEST_NAME):
         kvpHeaders.addPair(Message.KEY_REQUEST_NAME, self.kvpHeaders.getValue(Message.KEY_REQUEST_NAME))
      else:
         kvpHeaders.addPair(Message.KEY_REQUEST_NAME, "")

      kvpHeaders.addPair(Message.KEY_PAYLOAD_LENGTH, str(len(payload)))
      
      headersAsString = self.kvpToString(kvpHeaders)

      headerLengthPrefix = self.encodeLength(len(headersAsString))
      headerLengthPrefix = StrUtils.padRight(headerLengthPrefix, ' ', Message.NUM_CHARS_HEADER_LENGTH)

      messageAsString = ""
      messageAsString += headerLengthPrefix
      messageAsString += headersAsString
      messageAsString += payload
   
      return messageAsString
   
   '''
    * Flatten a KeyValuePairs object as part of flattening the Message
    * @param kvp the KeyValuePairs object whose string representation is needed
    * @return the string representation of the KeyValuePairs object
   '''
   @staticmethod
   def kvpToString(kvp):
      kvpAsString = ""
   
      if (kvp is not None) and not kvp.empty():
         keys = kvp.getKeys()
         i = 0
      
         for key in keys:
            if i > 0:
               # append pair delimiter
               kvpAsString += Message.DELIMITER_PAIR
         
            kvpAsString += key
            kvpAsString += Message.DELIMITER_KEY_VALUE
            kvpAsString += kvp.getValue(key)
         
            i += 1
      
      return kvpAsString
   
   '''
    * Reconstitutes the state of a KeyValuePairs from the specified string
    * @param s the textual data that holds the KeyValuePairs state data
    * @param kvp the KeyValuePairs object instance to populate
    * @return boolean indicating whether any state data was populated
    * @see KeyValuePairs()
   '''
   @staticmethod
   def fromString(s, kvp):
      numPairsAdded = 0
      
      if (s is not None) and (kvp is not None) and (len(s) > 0):
         stPairs = StringTokenizer(s, Message.DELIMITER_PAIR)
         numPairs = stPairs.countTokens()
      
         if numPairs > 0:
            while stPairs.hasMoreTokens():
               keyValuePair = stPairs.nextToken()
            
               stKeyValue = StringTokenizer(keyValuePair, Message.DELIMITER_KEY_VALUE)
               numTokens = stKeyValue.countTokens()
               if numTokens == 2:
                  key = stKeyValue.nextToken()
                  value = stKeyValue.nextToken()
                  kvp.addPair(key, value)
                  numPairsAdded += 1
               else:
                  Logger.debug("Message.fromString: numTokens = " + str(numTokens))
         else:
            Logger.debug("Message.fromString: no pairs found with string tokenizer")
      else:
         Logger.debug("Message.fromString: condition failed: (s is not None) and (kvp is not None) and (len(s) > 0)")
   
      return numPairsAdded > 0
   
   '''
    * Encodes a length to a string so that it can be encoded in flattened message (used internally)
    * @param lengthBytes the length in bytes to encode
    * @return the string representation of the length
   '''
   @staticmethod
   def encodeLength(lengthBytes):
      return StrUtils.padRight(str(lengthBytes), ' ', Message.NUM_CHARS_HEADER_LENGTH)
   
   '''
    * Decodes the length of the message header by reading from a socket (used internally)
    * @param socketConnection the socket to read from
    * @return the decoded length of the message header
    * @see Socket()
   '''
   @staticmethod
   def decodeLength(socketConnection):
      lengthBytes = 0
   
      if socketConnection is not None:
         lengthAsChars = socketConnection.readSocket(Message.NUM_CHARS_HEADER_LENGTH)
         if len(lengthAsChars) == Message.NUM_CHARS_HEADER_LENGTH:
            encodedLength = String(lengthAsChars)
            return int(encodedLength)
   
      return lengthBytes
   
   '''
    * Retrieves a socket connection for the specified service (used internally)
    * @param serviceName the name of the service whose connection is needed
    * @return a Socket instance on success, None on failure
   '''
   @staticmethod
   def socketForService(serviceName):
      if Messaging.isInitialized():
         messaging = Messaging.getMessaging()
      
         if messaging is not None:
            if messaging.isServiceRegistered(serviceName):
               serviceInfo = messaging.getInfoForService(serviceName)
               name = serviceInfo.getServiceName()
               host = serviceInfo.getHost()
               port = serviceInfo.getPort()
               s = None
               try:
                  s = Socket(host, port)
               except IOError:
                  s = None
               
               return s
            else:
               Logger.error("Message.socketForService: service is not registered")
         else:
            Logger.error("Message.socketForService: Messaging.getMessaging returned None, but isInitialized returns true")
      else:
         Logger.error("Message.socketForService: messaging not initialized")
   
      return None

#******************************************************************************
#******************************************************************************

