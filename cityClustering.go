package main

import (
	"context"
	`encoding/csv`
	`fmt`
	`log`
	`os`
	"time"

	`go.mongodb.org/mongo-driver/bson`
	"go.mongodb.org/mongo-driver/bson/primitive"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type Intersection struct {
	ID                             primitive.ObjectID `bson:"_id,omitempty"`
	Type                           string             `bson:"type,omitempty"`
	Geometry                       Geometry           `bson:"geometry,omitempty"`
	Lat                            float64            `bson:"lat,omitempty"`
	Lon                            float64            `bson:"lon,omitempty"`
	ClusterNumber                  int                `bson:"clusterNumber,omitempty"`
	NumberOfIntersectionsInCluster int                `bson:"numberOfIntersectionsInCluster,omitempty"`
}

type Geometry struct {
	Coordinates []float64 `bson:"coordinates,omitempty"`
}

//var radiuses = []float32{150, 200}
var radiuses = []float32{60, 70, 80, 90, 100, 110, 120, 150}
//70, 80, 90, 100, 110, 120, 150
//30, 60, 120, 250, 350, 450, 550, 700, 850, 1000, 1500, 2000
//10, 20, 40, 50, 70, 80, 90, 100, 110,
//, 120, 70, 80, 90, 100, 150, 250
var collectionName = "netivot_no_pedestrians"

func main() {
	ctx, _ := context.WithTimeout(context.Background(), 240*time.Minute)
	client, err := mongo.Connect(ctx, options.Client().ApplyURI("mongodb://127.0.0.1:27017"))
	if err != nil {
		panic(err)
	}
	defer client.Disconnect(ctx)

	database := client.Database("osm")
	intersectionsCollection := database.Collection(collectionName)

	for radius := range radiuses {
		findGreatestClusterAndSaveToCollection(intersectionsCollection, radius)
	}
}

func findGreatestClusterAndSaveToCollection(intersectionsCollection *mongo.Collection, currentRadius int) {
	ctx, _ := context.WithTimeout(context.Background(), 240*time.Minute)
	prepareCollection(intersectionsCollection, ctx)
	clusterNumber := 1
	for ; ; {
		var firstIntersection = Intersection{}
		singleResult := intersectionsCollection.FindOne(ctx, bson.M{"clusterNumber": 0}).Decode(&firstIntersection)
		if singleResult != nil && singleResult.Error() == "mongo: no documents in result" {
			fmt.Println("Done!")
			break
		}

		updateClusterNumber(ctx, intersectionsCollection, firstIntersection.ID, clusterNumber)

		createClusterRecursively(ctx, firstIntersection, intersectionsCollection, clusterNumber, radiuses[currentRadius])
		setNumberOfIntersectionsInClusterForAllIntersections(clusterNumber, ctx, intersectionsCollection)

		clusterNumber++
		fmt.Println("Finished clusterNumber", clusterNumber)
	}

	saveClusterToCSV(intersectionsCollection, radiuses[currentRadius])
}

func saveClusterToCSV(collection *mongo.Collection, radius float32) {
	path, err := os.Getwd()
	if err != nil {
		log.Println(err)
	}
	fileName := fmt.Sprintf(
		"%s/results/%s_full_collection_cluster_%f.csv",
		path, collectionName, radius)
	file, err := os.Create(fileName)
	if err != nil {
		panic(fmt.Sprintf("Cannot create file: %s", err))
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()
	err = writer.Write([]string{"id", "lat", "lon", "clusterNumber", "numberOfIntersectionsInCluster"})
	if err != nil {
		panic(fmt.Sprintf("Cannot write to file: %s", err))
	}

	cur, err := collection.Find(nil, bson.M{"numberOfIntersectionsInCluster": bson.M{"$gte": 10}})
	for cur.Next(nil) {
		var p Intersection
		err := cur.Decode(&p)
		if err != nil {
			panic(fmt.Sprintf("Could not decode point for insertion to new cluster: %s", err.Error()))
		}
		p.Lon = p.Geometry.Coordinates[0]
		p.Lat = p.Geometry.Coordinates[1]
		intersectionString := convertIntersectionStructToCSV(p)

		err = writer.Write(intersectionString)
		if err != nil {
			panic(fmt.Sprintf("Cannot write to file: %s", err))
		}
	}
}

func prepareCollection(collection *mongo.Collection, ctx context.Context) {
	update := bson.M{
		"$set": bson.M{
			"numberOfIntersectionsInCluster": 0,
			"clusterNumber":                  0,
		},
	}
	_, err := collection.UpdateMany(
		ctx,
		bson.M{},
		update,
	)

	if err != nil {
		panic(err)
	}

	//if updateResponse.ModifiedCount != 237794 {
	//	panic(fmt.Sprintf("Updated %d documents instead of %d", updateResponse.ModifiedCount, 237794))
	//}
}

func setNumberOfIntersectionsInClusterForAllIntersections(clusterNumber int, ctx context.Context, collection *mongo.Collection) {
	count, err := collection.CountDocuments(ctx, bson.M{"clusterNumber": clusterNumber})
	if err != nil {
		panic(err)
	}

	update := bson.M{
		"$set": bson.M{
			"numberOfIntersectionsInCluster": count,
		},
	}
	updateResponse, err := collection.UpdateMany(
		ctx,
		bson.M{"clusterNumber": clusterNumber},
		update,
	)

	if err != nil {
		panic(err)
	}

	if updateResponse.ModifiedCount != count {
		panic(fmt.Sprintf("Updated %d documents instead of %d", updateResponse.ModifiedCount, count))
	}
}

func createClusterRecursively(ctx context.Context, intersection Intersection, collection *mongo.Collection, clusterNumber int, radius float32) {
	filter := bson.D{
		{"geometry",
			bson.D{
				{"$geoWithin", bson.D{
					{"$centerSphere", bson.A{intersection.Geometry.Coordinates, kilometersToRadians(radius/1000)}},
				}},
			}},
		{"clusterNumber", 0},
	}

	cur, err := collection.Find(ctx, filter)
	if err != nil {
		panic(err)
	}

	var results []Intersection
	for cur.Next(ctx) {
		var p Intersection
		err := cur.Decode(&p)
		if err != nil {
			panic("Could not decode Point")
		}
		results = append(results, p)
	}

	if len(results) == 0 {
		return
	}

	resultsId := extractIds(results)

	filterUpdate := bson.M{"_id": bson.M{"$in": resultsId}}
	updateQuery := bson.D{
		{"$set", bson.D{{"clusterNumber", clusterNumber}}},
	}

	updateResult, err := collection.UpdateMany(ctx, filterUpdate, updateQuery)
	if err != nil {
		panic(err)
	}

	fmt.Println("len(results.csv)", len(results))
	fmt.Println("updateResult.MatchedCount", updateResult.MatchedCount)

	for _, intersection := range results {
		createClusterRecursively(ctx, intersection, collection, clusterNumber, radius)
	}

	return
}

func extractIds(results []Intersection) []primitive.ObjectID {
	ids := []primitive.ObjectID{}
	for _, result := range results {
		ids = append(ids, result.ID)
	}
	return ids
}

func updateClusterNumber(ctx context.Context, collection *mongo.Collection, intersectionId primitive.ObjectID, clusterNumber int) {
	_, err := collection.UpdateOne(
		ctx,
		bson.M{"_id": intersectionId},
		bson.D{
			{"$set", bson.D{{"clusterNumber", clusterNumber}}},
		},
	)

	if err != nil {
		panic(err)
	}
}

func kilometersToRadians(kilometers float32) float32 {
	return (kilometers * 0.621371) / 3963.2
}

func convertIntersectionStructToCSV(intersection Intersection) []string {
	lat := fmt.Sprintf("%f", intersection.Lat)
	lon := fmt.Sprintf("%f", intersection.Lon)
	clusterNumber := fmt.Sprintf("%d", intersection.ClusterNumber)
	numberOfIntersectionsInCluster := fmt.Sprintf("%d", intersection.NumberOfIntersectionsInCluster)
	return []string{intersection.ID.String(), lat, lon, clusterNumber, numberOfIntersectionsInCluster}
}
