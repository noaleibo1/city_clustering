package main

import (
	`context`
	`fmt`
	`time`

	`github.com/fogleman/delaunay`
	`go.mongodb.org/mongo-driver/bson`
	`go.mongodb.org/mongo-driver/mongo`
	`go.mongodb.org/mongo-driver/mongo/options`
)

func main_temp() {
	ctx, _ := context.WithTimeout(context.Background(), 240*time.Minute)
	client, err := mongo.Connect(ctx, options.Client().ApplyURI("mongodb://127.0.0.1:27017"))
	if err != nil {
		panic(err)
	}
	defer client.Disconnect(ctx)

	database := client.Database("osm")
	IntersectionsCollection := database.Collection("intersections_clean")
	TriangulateCollection := database.Collection("Triangulate")

	points := getPointsFromDB(IntersectionsCollection)
	// populate points...
	triangulation, err := delaunay.Triangulate(points)
	if err != nil {
		panic(err)
	}

	fmt.Println(triangulation.ConvexHull)

	writeResultToDB(TriangulateCollection, triangulation)
}

func writeResultToDB(collection *mongo.Collection, triangulation *delaunay.Triangulation) {
	ctx, _ := context.WithTimeout(context.Background(), 240*time.Minute)

	for _, t := range triangulation.ConvexHull {
		collection.InsertOne(ctx, t)
	}

}

func getPointsFromDB(collection *mongo.Collection) []delaunay.Point {
	ctx, _ := context.WithTimeout(context.Background(), 240*time.Minute)

	cur, err := collection.Find(ctx, bson.D{{"clusterNumber", 1}})
	if err != nil {
		panic(err)
	}

	var results []delaunay.Point
	for cur.Next(ctx) {
		var p Intersection
		err := cur.Decode(&p)
		if err != nil {
			panic("Could not decode Point")
		}
		results = append(results, delaunay.Point{
			X: p.Geometry.Coordinates[0],
			Y: p.Geometry.Coordinates[1],
		})
	}

	return results
}