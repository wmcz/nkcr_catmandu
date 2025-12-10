import codecs
class Logger:
    """
    Logger class responsible for managing log files for different entity types. It supports two types
    of logging: 'complete' and 'err'. The logs are written to separate files based on the log type,
    and the class ensures loading file contents only once per instance. Also includes functionality
    to check if a specific entry exists in the logs.

    :ivar entityType: Represents the type of entity for which the logs are maintained (e.g., user,
        process).
    :type entityType: str
    :ivar logType: Specifies the type of log being used. Defaults to 'complete'. If set to 'err', it
        records error logs with additional error text.
    :type logType: str
    :ivar loadedSet: A set to maintain already logged entries for quick lookup. Avoids re-logging the
        same entries multiple times.
    :type loadedSet: set
    """
    def __init__(self, entityType, logType='complete'):
        """
        Initializes an instance of the class to manage and load files related to a specific entity type
        and log type. The object ensures that files are loaded only once during its lifecycle.

        :param entityType: The type of entity for which the operation is performed.
        :type entityType: Any
        :param logType: Indicates the type of log associated with the operation. Defaults to 'complete'.
        :type logType: str

        :raises FileNotFoundError: If an attempt to load a file fails due to it not being found.
        """
        self.entityType = entityType
        self.logType = logType
        self.loadedSet = set()
        try:
            self.loadFileOnce()
        except FileNotFoundError as e:
            self.isCompleteFile('first')

    def log(self, text, errorText = False, printLog = True):
        """
        Logs a given message based on the log type and optional parameters. Supports
        error handling and printing the log if specified.

        :param text: The main text message to log.
        :type text: str
        :param errorText: An optional error description appended to the log text
            if the log type is 'err'. Default is False.
        :type errorText: str or bool
        :param printLog: A boolean flag indicating whether to print the log.
            Default is True.
        :type printLog: bool
        :return: None
        """
        if self.logType == 'err':
            if errorText:
                text = text + ';' + errorText
            self.logError(text)
        else:
            self.logComplete(text)

        self.text = text
        if printLog:
            self.printLog()

    def printLog(self):
        """
        Prints the log message based on the type of log (error or normal).

        If the log type is 'err', the method splits the log text by ';' and prints
        an error message with the second and first segments in the format:
        'Error: <second_segment> - <first_segment>'.

        If the log type is not 'err', it is treated as normal, and the method prints
        the log text prefixed with 'OK: '.

        :raises AttributeError: If the attributes `logType` or `text` are not set
            in the object.
        """
        if self.logType == 'err':
            splits = self.text.split(';')
            print('Error: ' + splits[1] + ' - ' + splits[0])
        else:
            print('OK: ' + self.text)

    def logComplete(self, title):
        """
        Writes a title to a log file specific to the entity type and updates the loaded set.

        This method appends the provided title to a log file named after the entity type of
        the object and ensures the title is tracked in the loaded set for record-keeping.

        :param title: The title string to be written to the log file.
        :type title: str
        :return: None
        """
        file = codecs.open("log_" + self.entityType + ".txt", "a", "utf-8")
        file.write(title + '\n')
        self.loadedSet.add(title)
        file.close()

    def logError(self, title):
        """
        Logs an error message to a file specific to the entity type of the instance.

        This method appends the provided error message to a log file named
        "log_err_<entityType>.txt", where <entityType> is replaced with the
        specific entity type of the instance.

        :param title: The error message to log.
        :type title: str
        :return: None
        """
        file = codecs.open("log_err_" + self.entityType + ".txt", "a", "utf-8")
        file.write(title + '\n')
        file.close()

    def loadFileOnce(self):
        """
        Loads a file containing logs for a specific entity type and populates a set
        with the entries. The file is only loaded once to avoid redundant processing.
        Each entry in the file is stripped of leading/trailing whitespace and added
        to the set.

        :param self: Reference to the current instance of the class.

        :return: None
        """
        f = codecs.open("log_" + self.entityType + ".txt")
        lines = f.readlines()
        for line in lines:
            self.loadedSet.add(line.strip())
        f.close()

    def isCompleteFile(self, entity):
        """
        Determines whether a specified entity is fully processed and marked as complete.

        This function checks whether the given entity exists in the `loadedSet` attribute,
        indicating that it has been fully processed. If the entity is found in the set, it
        returns ``True``; otherwise, it returns ``False``. The method handles potential
        IOError exceptions which might occur during file operations. However, in the event
        that a file is missing, the function attempts to create a new file with the appropriate
        name.

        :param entity: The entity to be checked for completeness.
        :type entity: Any
        :return: ``True`` if the entity is found in the `loadedSet`, otherwise ``False``.
        :rtype: bool
        """
        try:
            pass
            # file = codecs.open("log_" + self.entityType + ".txt", "r", "utf-8")
            # lines = file.readlines()
            # list_entity = list()
            # file.close()

            # setLines = set(lines)
            if entity in self.loadedSet:
                return True
            else:
                return False
            #TODO opravit!

            # try:
            #     list_entity.index(entity)
            #     return True
            # except ValueError:
            #     return False
        except IOError as e:
            print("Not exist file")
            codecs.open('log_' + self.entityType + '.txt', 'w', 'utf-8')
