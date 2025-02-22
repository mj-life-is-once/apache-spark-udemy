from pyspark import SparkConf, SparkContext
from typing import Tuple, List

conf = SparkConf().setMaster("local").setAppName("DegreesOfSeparation")
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")

# The characters we wish to find the degree of separation between:
startCharacterID = 5306  # SpiderMan
targetCharacterID = 14  # ADAM 3,031 (who?)

# Our accumulator, used to signal when we find the target character during
# our BFS traversal.
hitCounter = sc.accumulator(0)


def convertToBFS(line: str):
    fields = line.split()  # default split with " "
    heroID = int(fields[0])
    connections = []
    for i in range(1, len(fields)):
        connections.append(int(fields[i]))

    color = "WHITE"
    distance = 9999

    if heroID == startCharacterID:
        color = "GRAY"
        distance = 0

    return (heroID, (connections, distance, color))


def createStartingRdd():
    inputFile = sc.textFile("data/Marvel-graph.txt")
    return inputFile.map(convertToBFS)


def bfsMap(node: Tuple[int, Tuple[List[int], int, str]]):
    # unpack node
    characterID, data = node
    connections, distance, color = data

    results = []

    # skip Black nodes (already visited node)
    # GRAY: node to process
    if color == "GRAY":
        for newCharacterID in connections:
            # if newCharaterID matches target, add counter to break the loop
            if targetCharacterID == newCharacterID:
                hitCounter.add(1)

            # start coloring the node with GRAY
            newEntry = (newCharacterID, ([], distance + 1, "GRAY"))
            results.append(newEntry)

        # if we processed the node, mark it BLACK
        color = "BLACK"

    # Emit the input node so we don't lose it
    results.append((characterID, (connections, distance, color)))
    return results


def bfsReduce(data1: Tuple[List[int], int, str], data2: Tuple[List[int], int, str]):
    # reduce function reduceByKey
    edges1, distance1, color1 = data1
    edges2, distance2, color2 = data2

    distance = 9999
    color = color1
    edges = []

    # See if one is the original node with its connections.
    # If so preserve them.
    if len(edges1) > 0:
        edges.extend(edges1)
    if len(edges2) > 0:
        edges.extend(edges2)

    # Preserve minimum distance
    if distance1 < distance:
        distance = distance1

    if distance2 < distance:
        distance = distance2

    # Preserce the darkest color
    if color1 == "WHITE" and (color2 == "GRAY" or color2 == "BLACK"):
        color = color2

    if color1 == "GRAY" and color2 == "BLACK":
        color = color2

    if color2 == "WHITE" and (color1 == "GRAY" or color1 == "BLACK"):
        color = color1

    if color2 == "GRAY" and color1 == "BLACK":
        color = color1

    return (edges, distance, color)


# Main Program
iterationRdd = createStartingRdd()

# Run BFS
for iteration in range(0, 10):
    print("Running BFS iteration# " + str(iteration + 1))

    # Create new vertices as needed to darken or reduce distances in the
    # reduce stage. If we encounter the node we're looking for as a GRAY
    # node, increment our accumulator to signal that we're done.
    mapped = iterationRdd.flatMap(bfsMap)

    # Note that mapped.count() action here forces the RDD to be evaluated, and
    # that's the only reason our accumulator is actually updated.
    print("Processing " + str(mapped.count()) + " values.")

    if hitCounter.value > 0:
        print(
            "Hit the target character! From "
            + str(hitCounter.value)
            + " different direction(s)."
        )
        break

    # Reducer combines data for each character ID, preserving the darkest
    # color and shortest path.
    iterationRdd = mapped.reduceByKey(bfsReduce)

# print(iterationRdd.collect())
