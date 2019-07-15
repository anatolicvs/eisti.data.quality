from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, FloatType

conf = SparkConf().setAppName("eisti::dq::app") \
                  .setMaster("spark://172.23.0.2:7077")


sc = SparkContext(conf=conf)

spark = SparkSession.builder \
    .config(conf=conf) \
    .getOrCreate()


def save_json(document, schema, path, saveMode='overwrite'):
    """
        Save an RDD or a dataframe into a.json file with saveMode specified.

        Args:
            document(rdd): THe document to save.
            schema(list): The schema of the columns of the rdd, used to convert to Dataframe.
            path(string): The path in which the document have to be saved.
            saveMode(string) A string repreenting the save mode, like 'overwrite' and 'append'
    """

    if saveMode is None:
        try:
            print("Saving Json...")
            if schema is None:
                document.coalesce(1).write.json(path)
            else:
                document.toDF(schema).coalesce(1).write.json(path)
        except Exception as e:
            print("The file already exists")
            print(e)
    else:
        print("Modifying Json...")
        if schema is None:
            document.coalesce(1).write.mode(saveMode).json(path)
        else:
            document.toDF(schema).coalesce(1).write.mode(saveMode).json(path)


def save_txt(document, path):
    """
    Save an Rdd into a .txt file if it does not already exists

    Args:
        document(rdd): The document to save.
        path(string): The path in which the document have to be saved
    """
    try:
        document.coalesce(1).saveAsTextFile(path)
    except Exception:
        # the file has already been saved for the current data source
        pass


def extract_main_elements_xml(document):
    """
    This function search in the xml data the main elements: separators, elements and timestamps and return a new rdd

    Args:
        document(line): The line of the rdd to parse.
    """
    matchObj = re.findall(r'<separator>(.*)</separator>', document)
    if matchObj:
        return matchObj[0]

    matchObj = re.findall(r'<element>(.*)</element>', document)
    if matchObj:
        return "element"

    matchObj = re.findall(r'<timestamp>(.*)</timestamp>', document)
    if matchObj:
        return "element"


def extract_header(document):
    """
    This function allow to extract the header of the columns from the xml if available

    Args:
        document(line): The line of the rdd to parse.
    """
    matchObj = re.findall(r'<header>(.*)</header>', document)
    if matchObj:
        return matchObj[0]


def extract_timestamp_format(document):
    """
    This function allow to extract only the timestamps' columns from the xml

    Args:
        document(line): The line of the rdd to parse.
    """
    matchObj = re.findall(r'<timestamp>(.*)</timestamp>', document)
    if matchObj:
        return matchObj[0]


def regular_parsing_xml(document):
    """
    Main function to extract the regular expression from the xml to be used to derive the elements for the analysis - It must be upgraded

    Args:
        document(line): The line of the rdd to parse.
    """
    prev = ""
    next = False

    prec = ""
    post = ""
    # When each element is found, the antecedent and consequent separators are
    # saved in strings
    for s in document.toLocalIterator():
        if(next):
            post = post + str(s)
            next = False
        else:
            if(str(s) == "element"):
                prec = prec + str(prev)
                next = True
        prev = str(s)

    prec = ''.join(set(prec))
    post = ''.join(set(post))
    # Construct the final regular expression
    regString = "[" + prec + "]" + "(.*?)" + "[" + post + "]"
    regString = regString.replace('"', '')
    return regString


"""
DEFINITION STEP - DOCUMENT STRUCTURATION
"""


def regular_parsing(document, regex):
    """
    Main function to derive all the elements that respects the regex String

    Args:
        document(line): The line of the rdd to parse.
        regex(string): The string that contains the regular expression
    """
    return re.findall(regex, document)


def escape_removal(document):
    """
    Function to remove the escape from the elements of the file, the function should be upgraded to eliminate all the undesired symbols

    Args:
        document(line): The line of the rdd to parse.
    """
    return re.sub(r'\\', "", document)


def quote_removal(document):
    """
    Function to remove the double quotes from the data

    Args:
        document(line): The line of the rdd to parse.
    """
    return re.sub(r'"', "", document)


def comma_to_dot_number_conversion(document):
    """
    Function to convert the numbers with ,-separation to .-separated

    Args:
        document(line): The line of the rdd to parse.
    """
    return re.sub(r'(\d+),(\d+)', r'\1.\2', document)


"""
DEFINITION STEP - DIMENSION ANALYSIS
"""


def restriction_filter(document, newIndexes, j=None):
    """
    The function performs the union of all the values of the different attributes into a single string, returning the final string

    Args:
        document(line): The line of the rdd to parse.
            newIndexes(list): List of index of the elements to union in a single string
            j(int): Index of the dependent element that should be returned as a value of the key-value pair
    """
    for h in range(1, len(newIndexes)):
        document[newIndexes[0]] = str(
            document[newIndexes[0]]) + "," + str(document[newIndexes[h]])

    if j is not None:
        return (document[newIndexes[0]], document[j])

    return (document[newIndexes[0]])


def accuracy(sc, sqlContext, document, columns, dataTypes, volatiliy, desiredColumns, resultFolder, dimensionColumn, columnsKey, meanAccuracyValues, devAccuracyValues, timelinessAnalyzer, completenessAnalyzer, distinctnessAnalyzer, populationAnalyzer, inputSource, associationRules, totVolume, granularityDimensions, performanceSample):
    """
    This function calculate the dimension of accuracy for each numerical attribute, save the results and return the Global values

    The arguments are all the same for each quality dimension since they are called dynamically in a cycle
    Args:
            sc: SparkContext of the sparkSession
            sqlContext: sqlContext of the sparkSession
            document(rdd): The rdd of the source.
            dataTypes(list): List of the types of the attribute based on the position in the rdd
            volatility(float or string): Parameter necessary for Timeliness and Completeness_Frequency: Number of hours to consider the data still recent
            desiredColumns(list): Indexes of the attributes requested in the analysis
            resultFolder(string): Absolute path of the destination of all the saved files
            dimensionColumn(dict): Dictionary containing the allowed index of the attributes for each available quality dimension
            columnsKey(list): Indexes of the attributes to consider as a grouping key
            meanAccuracyValues(list): Parameter necessary for Accuracy: List of the mean values to consider for each columnsKey requested
            devAccuracyValues(list): Parameter necessary for Accuracy: List of the allowed intervals values to consider for each columnsKey requested based on the previous considered mean values
            timelinessAnalyzer(boolean): Value set to True if the Timeliness dimension has to be evaluated
            completenessAnalyzer(boolean): Value set to True if the Completeness_Frequency dimension has to be evaluated
            distinctnessAnalyzer(boolean): Value set to True if the Distinctness dimension has to be evaluated
            populationAnalyzer(boolean): Value set to True if the Completeness_Population dimension has to be evaluated
            inputSource(string): Absolute path of the position of the source folder containing the Profiling information and all the portion of profiled data
            associationRules(list): Parameter optional for Consistency: List of additional association rules to be considered in the analysis
            totVolume(int): Number of total rows of the source
            granularityDimensions(dict): Dictionary containing the requested degrees of granularity for each requested quality dimension
            performanceSample(float): Number from 0+ to 1 representing the portion of data that has been considered in the analysis
    """

    print(" ")
    print("Accuracy")
    print(" ")

    finalAccuracyRdd = sc.emptyRDD()
    attrName = []
    attrAccuracy = []
    confidence = []
    counter = 0

    # get the desired degrees of granularity
    try:
        granularity = granularityDimensions["Accuracy"].split(",")
        globalAcc = True if "global" in granularity else False
        attributeAcc = True if "attribute" in granularity else False
        valueAcc = True if "value" in granularity else False

        if granularity[0] == "":
            globalAcc = True
    except Exception:
        globalAcc = True
        attributeAcc = False
        valueAcc = False

    for j in columns:

        print("-Numerical Attribute = " + str(desiredColumns[j]))
        globalIncluded = 0

        meanAccuracy = meanAccuracyValues[counter]
        devAccuracy = devAccuracyValues[counter]
        counter = counter + 1

        if attributeAcc or valueAcc:

            for stringIndex in columnsKey:

                # key columns = keyColumns
                stringSplitIndex = stringIndex.split(",")
                newIndexes = [desiredColumns.index(
                    k) for k in stringSplitIndex]
                newDocument = document.map(
                    lambda x: restriction_filter(x, newIndexes, j))
                stringAttribute = '_'.join(stringSplitIndex)

                try:
                    # it is useless to group by the timestamps
                    if not set(newIndexes).isdisjoint(dimensionColumn["Timeliness"]):
                        continue

                except Exception:
                    pass

                if j in newIndexes:
                    continue

                print("--Key Attribute = " + stringAttribute)

                # calculate the distance between each value and the expected
                # mean, and calculate each accuracy value as this distance
                # divided by the maximum allowed interval
                staticAccuracyRdd = (newDocument.map(lambda x: (x[0], max(0.0, 1 - abs((float(x[1].replace(',', '.')) - float(meanAccuracy.replace(',', '.'))) / (float(devAccuracy.replace(',', '.')) / 2)))))
                                     .map(lambda x: (x[0], (1, x[1], ceil(x[1]))))
                                     .reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1], x[2] + y[2]))
                                     )
                if globalAcc:
                    globalIncluded = 1
                    globalAccuracies = (staticAccuracyRdd.map(lambda x: (x[1][0], x[1][1], x[1][2]))
                                        .reduce(lambda x, y: (x[0] + y[0], x[1] + y[1], x[2] + y[2]))
                                        )
                    attributeStaticAccuracy = globalAccuracies[
                        2] / float(globalAccuracies[0])
                    attributeAccuracy = globalAccuracies[
                        1] / float(globalAccuracies[0])

                accuracyRdd = staticAccuracyRdd.map(lambda x: (
                    x[0], x[1][1] / float(x[1][0]), x[1][2] / float(x[1][0])))

                print("Calculate for each record the accuracy value as 1 - (( Value - desired Mean )/(Maximum interval / 2)) and find the mean results per Attribute's Value: -> ( Attribute's Value, Mean Accuracy )")
                # print(accuracyRdd.take(5))

                if valueAcc:
                    save_json(accuracyRdd, ["Value", "AccuracyDynamic", "AccuracyStatic"], resultFolder +
                              "/accuracy_values/attribute_" + stringAttribute + "_REF_" + str(desiredColumns[j]))

                if attributeAcc:
                    finalAccuracyRdd = finalAccuracyRdd.union(sc.parallelize([(stringAttribute, accuracyRdd.map(
                        lambda x: x[1]).mean(), accuracyRdd.map(lambda x: x[2]).mean(), performanceSample)]))

            if attributeAcc:
                # save file into hdfs
                save_json(finalAccuracyRdd, ["Attribute", "AccuracyDynamic", "AccuracyStatic",
                                             "Confidence"], resultFolder + "/accuracy_attributes_" + str(desiredColumns[j]))

        # append the global value to the list
        if globalAcc:
            if not globalIncluded:
                # calculate final accuracies
                globalAccuracies = (document.map(lambda x: (max(0.0, 1 - abs((float(x[j].replace(',', '.')) - float(meanAccuracy.replace(',', '.'))) / (float(devAccuracy.replace(',', '.')) / 2)))))
                                    .map(lambda x: (1, x, ceil(x)))
                                    .reduce(lambda x, y: (x[0] + y[0], x[1] + y[1], x[2] + y[2]))
                                    )

                attributeStaticAccuracy = globalAccuracies[
                    2] / float(globalAccuracies[0])
                attributeAccuracy = globalAccuracies[
                    1] / float(globalAccuracies[0])

            attrAccuracy.append(attributeStaticAccuracy)
            attrName.append("Accuracy_Static_" + str(desiredColumns[j]))
            confidence.append(performanceSample)

            attrAccuracy.append(attributeAccuracy)
            attrName.append("Accuracy_Dynamic_" + str(desiredColumns[j]))
            confidence.append(performanceSample)

            print("Global Static Accuracy " + str(attributeStaticAccuracy))
            print("Global Dynamic Accuracy " + str(attributeAccuracy))

    return attrName, attrAccuracy, confidence


def calculateSampleDeviation(line):
    # lambda (key,(sumDev,count,sumMean)):
    # (key,((sumDev/(count-1))**0.5,sumMean/count,count))
    try:
        newLine = (line[0], ((line[1][0] / (line[1][1] - 1))**0.5,
                             line[1][2] / float(line[1][1]), float(line[1][1])))
    except Exception:
        newLine = (line[0], (0, line[1][2] /
                             float(line[1][1]), float(line[1][1])))

    return newLine


def precision(sc, sqlContext, document, columns, dataTypes, volatiliy, desiredColumns, resultFolder, dimensionColumn, columnsKey, meanAccuracyValues, devAccuracyValues, timelinessAnalyzer, completenessAnalyzer, distinctnessAnalyzer, populationAnalyzer, inputSource, associationRules, totVolume, granularityDimensions, performanceSample):
    """
    This function calculate the dimension of precision for each numerical attribute, save the results and return the Global values

    The arguments are all the same for each quality dimension since they are called dynamically in a cycle
    Args:
            sc: SparkContext of the sparkSession
            sqlContext: sqlContext of the sparkSession
            document(rdd): The rdd of the source.
            dataTypes(list): List of the types of the attribute based on the position in the rdd
            volatility(float or string): Parameter necessary for Timeliness and Completeness_Frequency: Number of hours to consider the data still recent
            desiredColumns(list): Indexes of the attributes requested in the analysis
            resultFolder(string): Absolute path of the destination of all the saved files
            dimensionColumn(dict): Dictionary containing the allowed index of the attributes for each available quality dimension
            columnsKey(list): Indexes of the attributes to consider as a grouping key
            meanAccuracyValues(list): Parameter necessary for Accuracy: List of the mean values to consider for each columnsKey requested
            devAccuracyValues(list): Parameter necessary for Accuracy: List of the allowed intervals values to consider for each columnsKey requested based on the previous considered mean values
            timelinessAnalyzer(boolean): Value set to True if the Timeliness dimension has to be evaluated
            completenessAnalyzer(boolean): Value set to True if the Completeness_Frequency dimension has to be evaluated
            distinctnessAnalyzer(boolean): Value set to True if the Distinctness dimension has to be evaluated
            populationAnalyzer(boolean): Value set to True if the Completeness_Population dimension has to be evaluated
            inputSource(string): Absolute path of the position of the source folder containing the Profiling information and all the portion of profiled data
            associationRules(list): Parameter optional for Consistency: List of additional association rules to be considered in the analysis
            totVolume(int): Number of total rows of the source
            granularityDimensions(dict): Dictionary containing the requested degrees of granularity for each requested quality dimension
            performanceSample(float): Number from 0+ to 1 representing the portion of data that has been considered in the analysis
    """

    print(" ")
    print("Precision")
    print(" ")

    finalPrecisionRdd = sc.emptyRDD()
    attrPrecision = []
    attrName = []
    confidence = []

    # get the desired degrees of granularity
    try:
        granularity = granularityDimensions["Precision"].split(",")
        globalPrec = True if "global" in granularity else False
        attributePrec = True if "attribute" in granularity else False
        valuePrec = True if "value" in granularity else False

        if granularity[0] == "":
            globalPrec = True
    except Exception:
        globalPrec = True
        attributePrec = False
        valuePrec = False

    for j in columns:
        print("-Numerical Attribute = " + str(desiredColumns[j]))

        reShift = False
        statsDoc = document.map(lambda x: (
            float(x[j].replace(',', '.')))).stats()
        devAttribute = float(statsDoc.stdev())
        meanAttribute = float(statsDoc.mean())
        minValue = float(statsDoc.min())
        maxValue = float(statsDoc.max())

        if (minValue < 0) & (maxValue > 0):
            reShift = True

        if attributePrec or valuePrec:

            for stringIndex in columnsKey:

                # key columns = keyColumns
                stringSplitIndex = stringIndex.split(",")
                newIndexes = [desiredColumns.index(
                    k) for k in stringSplitIndex]
                newDocument = document.map(
                    lambda x: restriction_filter(x, newIndexes, j))
                stringAttribute = '_'.join(stringSplitIndex)

                try:
                    # it is useless to group by the timestamps
                    if not set(newIndexes).isdisjoint(dimensionColumn["Timeliness"]):
                        continue

                except Exception:
                    pass

                if j in newIndexes:
                    continue

                print("--Key Attribute = " + stringAttribute)

                keyFloatDocument = newDocument.map(
                    lambda x: (x[0], float(x[1].replace(',', '.'))))

                if reShift:
                    print("---Shifting all the values to make them greater than 1 ")
                    keyFloatDocument = keyFloatDocument.map(
                        lambda x: (x[0], x[1] + abs(minValue)))

                meanRdd = (keyFloatDocument.map(lambda x: (x[0], (x[1], 1)))
                           .reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))
                           .map(lambda x: (x[0], x[1][0] / float(x[1][1])))
                           )

                varRdd = (keyFloatDocument.join(meanRdd)
                          .map(lambda (key, (value, mean)): (key, ((value - mean)**2, 1, mean)))
                          .reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1], x[2] + y[2]))
                          .map(lambda (key, (sumDev, counter, sumMean)): (key, ((sumDev / counter)**0.5, sumMean / counter)))
                          )

                precisionRdd = varRdd.map(lambda (key, (dev, mean)): (
                    key, mean, max(0.0, 1.0 - dev / abs(mean)), dev))
                print(
                    "---Calculate value of precision per Attribute's value: --> Value, Mean, Precision, Standard Deviation")
                # print(precisionRdd.take(5))

                if reShift:
                    print(
                        "---Shifting all the values again to replace the correct mean")
                    precisionRdd = precisionRdd.map(
                        lambda (key, mean, prec, dev): (key, mean - abs(minValue), prec, dev))

                if valuePrec:
                    save_json(precisionRdd, ["Value", "Mean", "Precision", "StandardDeviation"], resultFolder +
                              "/precision_values/attribute_" + stringAttribute + "_number_" + str(desiredColumns[j]))

                finalPrecisionRdd = finalPrecisionRdd.union(sc.parallelize([(stringAttribute, precisionRdd.map(lambda x: x[1]).mean(
                ), precisionRdd.map(lambda x: x[2]).mean(), precisionRdd.map(lambda x: x[3]).mean(), performanceSample)]))

                print(" ")

        # calculate final aggregated value for precision

        if globalPrec:

            if reShift:
                attributePrecision = max(
                    0.0, 1.0 - devAttribute / (abs(meanAttribute) + abs(minValue)))
            else:
                attributePrecision = max(
                    0.0, 1.0 - devAttribute / abs(meanAttribute))

            attrPrecision.append(attributePrecision)
            attrName.append("Precision_" + str(desiredColumns[j]))
            confidence.append(performanceSample)
            attrPrecision.append(devAttribute)
            attrName.append("Precision(Deviation)_" + str(desiredColumns[j]))
            confidence.append(performanceSample)
            print("Global Precision " + str(attributePrecision))
        if attributePrec:
            print("--Final Aggregated File --> Attribute, Mean, Precision, Deviation")
            # print(finalPrecisionRdd.take(5))

            # save file into hdfs
            save_json(finalPrecisionRdd, ["Attribute", "Mean", "Precision", "Standard_Deviation",
                                          "Confidence"], resultFolder + "/precision_attributes_" + str(desiredColumns[j]))

    return attrName, attrPrecision, confidence


def mapping_completeness_missing(x, newIndexes):
    """
    This function remove empty,None or null value from each line, and return the key, the current line length the previous line length as a new Rdd line

    Args:
            x(Row): line of the Rdd
            newIndexes(list): list of indexes to union in a single element
    """
    for h in range(1, len(newIndexes)):
        x[newIndexes[0]] = str(x[newIndexes[0]]) + "," + str(x[newIndexes[h]])
        del x[newIndexes[h]]

    lineLength = len(x)
    previousline = x
    try:
        line = [el for el in previousline if el is not None]
        previousline = line
    except Exception:
        line = previousline

    previousline = line

    try:
        line = filter(lambda a: a != "", line)
        previousline = line
    except Exception:
        line = previousline

    previousline = line

    try:
        line = filter(lambda a: a != "nan", line)
        previousline = line
    except Exception:
        line = previousline

    previousline = line

    try:
        line = filter(lambda a: a != "null", line)
        previousline = line
    except Exception:
        line = previousline

    return (x[newIndexes[0]], (len(line) - 1, lineLength - 1))


def completeness_missing(sc, sqlContext, document, columns, dataTypes, volatiliy, desiredColumns, resultFolder, dimensionColumn, columnsKey, meanAccuracyValues, devAccuracyValues, timelinessAnalyzer, completenessAnalyzer, distinctnessAnalyzer, populationAnalyzer, inputSource, associationRules, totVolume, granularityDimensions, performanceSample):
    """
    This function calculate the value of the Part of Completeness regarding the missing elements per line,save the results and return the Global values

    The arguments are all the same for each quality dimension since they are called dynamically in a cycle
    Args:
            sc: SparkContext of the sparkSession
            sqlContext: sqlContext of the sparkSession
            document(rdd): The rdd of the source.
            dataTypes(list): List of the types of the attribute based on the position in the rdd
            volatility(float or string): Parameter necessary for Timeliness and Completeness_Frequency: Number of hours to consider the data still recent
            desiredColumns(list): Indexes of the attributes requested in the analysis
            resultFolder(string): Absolute path of the destination of all the saved files
            dimensionColumn(dict): Dictionary containing the allowed index of the attributes for each available quality dimension
            columnsKey(list): Indexes of the attributes to consider as a grouping key
            meanAccuracyValues(list): Parameter necessary for Accuracy: List of the mean values to consider for each columnsKey requested
            devAccuracyValues(list): Parameter necessary for Accuracy: List of the allowed intervals values to consider for each columnsKey requested based on the previous considered mean values
            timelinessAnalyzer(boolean): Value set to True if the Timeliness dimension has to be evaluated
            completenessAnalyzer(boolean): Value set to True if the Completeness_Frequency dimension has to be evaluated
            distinctnessAnalyzer(boolean): Value set to True if the Distinctness dimension has to be evaluated
            populationAnalyzer(boolean): Value set to True if the Completeness_Population dimension has to be evaluated
            inputSource(string): Absolute path of the position of the source folder containing the Profiling information and all the portion of profiled data
            associationRules(list): Parameter optional for Consistency: List of additional association rules to be considered in the analysis
            totVolume(int): Number of total rows of the source
            granularityDimensions(dict): Dictionary containing the requested degrees of granularity for each requested quality dimension
            performanceSample(float): Number from 0+ to 1 representing the portion of data that has been considered in the analysis
    """

    print(" ")
    print("Completeness_Missing")
    print(" ")

    lineLength = len(document.take(1)[0])

    finalCompleteRdd = sc.emptyRDD()

    # get the desired degrees of granularity
    try:
        granularity = granularityDimensions["Completeness_Missing"].split(",")
        globalMiss = True if "global" in granularity else False
        attributeMiss = True if "attribute" in granularity else False
        valueMiss = True if "value" in granularity else False

        if granularity[0] == "":
            globalMiss = True
    except Exception:
        globalMiss = True
        attributeMiss = False
        valueMiss = False

    if attributeMiss or valueMiss:

        for stringIndex in columnsKey:

            # key columns = keyColumns
            stringSplitIndex = stringIndex.split(",")
            newIndexes = [desiredColumns.index(k) for k in stringSplitIndex]
            newDocument = document.map(
                lambda x: restriction_filter(x, newIndexes))
            stringAttribute = '_'.join(stringSplitIndex)

            print("--Key Attribute = " + stringAttribute)

            itIsTime = False

            if valueMiss:
                try:
                    # it is useless to group by the timestamps
                    if not set(newIndexes).isdisjoint(dimensionColumn["Timeliness"]):
                        itIsTime = True

                except Exception:
                    pass

                if not itIsTime:
                    # analysis per value
                    print("--Key Attribute's Values Analysis")

                    keyValueDocument = document.map(
                        lambda x: mapping_completeness_missing(x, newIndexes))
                    print("---Find number of filtered and total elements per record, for each Key Attribute's Value: -> ( Key Attribute , ( Filtered Line Lenght , Full Line Lenght ) )")
                    # print(keyValueDocument.take(5))

                    # Add a filter to remove the null,none or empty keys
                    print("---Filter Null keys")
                    keyValueDocument = keyValueDocument.filter(lambda x: x[0] != "null").filter(lambda x: x[0] is not None).filter(
                        lambda x: x[0] != "").filter(lambda x: x[0] != "nan").reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))

                    keyValueDocument = keyValueDocument.map(lambda x: (
                        x[0], x[1][1] - x[1][0], x[1][0] / float(x[1][1])))
                    print(
                        "---Calculate Completeness Missing for each Attribute's Value: -> ( Value, Missing Values, Completeness Missing Value )")
                    # print(keyValueDocument.take(5))

                    save_json(keyValueDocument, ["Value", "MissingValues", "CompletenessMissingValue"],
                              resultFolder + "/completeness_missing_values/" + stringAttribute)

            if attributeMiss:
                # Analysis per attribute
                print("--Attribute's Analysis")

                # attribute elements
                attributeDocument = newDocument

                totElements = attributeDocument.count()
                print("---Total Elements")
                print(totElements)

                filteredElements = attributeDocument.filter(lambda x: x != "null").filter(
                    lambda x: x is not None).filter(lambda x: x != "").filter(lambda x: x != "nan").count()
                print("---Total Filtered Elements")
                print(filteredElements)

                completenessAttribute = filteredElements / float(totElements)
                print(completenessAttribute)

                # Save both values
                finalCompleteRdd = finalCompleteRdd.union(sc.parallelize(
                    [(stringAttribute, totElements - filteredElements, completenessAttribute, performanceSample)]))

    if attributeMiss:
        print("--Calculate value of Completeness Missing per Attribute: --> Attribute, Missing Values, Completeness Missing Value, Confidence")
        # print(finalCompleteRdd.take(5))

        # save file into hdfs
        save_json(finalCompleteRdd, ["Attribute", "MissingValues", "CompletenessMissingValue",
                                     "Confidence"], resultFolder + "/completeness_missing_attributes")

    if globalMiss:
        # Global Analysis
        print("-Global Missing Analysis")

        globalDocument = document.flatMap(lambda x: x)
        globalCount = globalDocument.count()
        filteredCount = globalDocument.filter(lambda x: x != "null").filter(
            lambda x: x is not None).filter(lambda x: x != "").filter(lambda x: x != "nan").count()

        qualityCompletenessMissing = filteredCount / float(globalCount)

        print("--Final Global Completeness Missing: " +
              str(qualityCompletenessMissing))
    else:
        qualityMissing = None

    return ["Completeness_Missing"], [qualityCompletenessMissing], [performanceSample]
