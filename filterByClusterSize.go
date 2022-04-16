package main

import (
	`context`
	`fmt`
	`strconv`
	`time`

	`go.mongodb.org/mongo-driver/bson`
	`go.mongodb.org/mongo-driver/mongo`
	`go.mongodb.org/mongo-driver/mongo/options`
)

type ClusterSize struct {
	ID                             string `bson:"_id,omitempty"`
	NumberOfIntersectionsInCluster string `bson:"count,omitempty"`
}

func main_temp2() {
	ctx, _ := context.WithTimeout(context.Background(), 60*time.Minute)
	client, err := mongo.Connect(ctx, options.Client().ApplyURI("mongodb://127.0.0.1:27017"))
	if err != nil {
		panic(err)
	}
	defer client.Disconnect(ctx)

	database := client.Database("osm")
	intersectionsCollection := database.Collection("intersections")
	filteredClustersSize := database.Collection("filteredClusterSize")

	cur, err := filteredClustersSize.Find(ctx, bson.M{})
	if err != nil {
		panic(err)
	}

	var clustersSizes []ClusterSize
	for cur.Next(ctx) {
		var p ClusterSize
		err := cur.Decode(&p)
		if err != nil {
			panic("Could not decode clusterSize")
		}
		clustersSizes = append(clustersSizes, p)
	}

	fmt.Println("clustersSizes", clustersSizes)

	for _, cluster := range clustersSizes {
		i, _ := strconv.Atoi(cluster.ID)
		fmt.Println(i)
		filter := bson.M{"clusterNumber": i}
		updateQuery := bson.D{
			{"$set", bson.D{{"numberOfIntersectionsInCluster", cluster.NumberOfIntersectionsInCluster}}},
		}
		updateResult, err := intersectionsCollection.UpdateMany(ctx, filter, updateQuery)
		if err != nil {
			panic(err)
		}
		fmt.Println("ModifiedCount", updateResult.ModifiedCount)
	}
}
