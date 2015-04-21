package mil.nga.giat.geowave.analytics.tools;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import mil.nga.giat.geowave.analytics.distance.CoordinateCircleDistanceFn;

import org.apache.commons.math3.geometry.euclidean.twod.Vector2D;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.LineString;
import com.vividsolutions.jts.geom.Point;

public class CurvedDensityDataGeneratorTool
{

	private static final CoordinateCircleDistanceFn DISTANCE_FN = new CoordinateCircleDistanceFn();

	public static final List<Point> generatePoints(
			final LineString line,
			final double distanceFactor,
			final int points ) {
		final List<Point> results = new ArrayList<Point>();
		Coordinate lastCoor = null;
		double distanceTotal = 0.0;
		final double[] distancesBetweenCoords = new double[line.getCoordinates().length - 1];
		int i = 0;
		for (final Coordinate coor : line.getCoordinates()) {
			if (lastCoor != null) {
				distancesBetweenCoords[i] = Math.abs(DISTANCE_FN.measure(
						lastCoor,
						coor));
				distanceTotal += distancesBetweenCoords[i++];
			}
			lastCoor = coor;
		}
		lastCoor = null;
		i = 0;
		for (final Coordinate coor : line.getCoordinates()) {
			if (lastCoor != null) {
				results.addAll(generatePoints(
						line.getFactory(),
						toVec(coor),
						toVec(lastCoor),
						distanceFactor,
						(int) ((points) * (distancesBetweenCoords[i++] / distanceTotal))));
			}
			lastCoor = coor;
		}

		return results;
	}

	private static final List<Point> generatePoints(
			final GeometryFactory factory,
			final Vector2D coordinateOne,
			final Vector2D coordinateTwo,
			final double distanceFactor,
			final int points ) {
		final List<Point> results = new ArrayList<Point>();
		final Random rand = new Random();
		final Vector2D originVec = coordinateTwo.subtract(coordinateOne);
		for (int i = 0; i < points; i++) {
			final double factor = rand.nextDouble();
			final Vector2D projectionPoint = originVec.scalarMultiply(factor);
			final double direction = rand.nextGaussian() * distanceFactor;
			final Vector2D orthogonal = new Vector2D(
					originVec.getY(),
					-originVec.getX());

			results.add(factory.createPoint(toCoordinate(orthogonal.scalarMultiply(
					direction).add(
					projectionPoint).add(
					coordinateOne))));
		}
		return results;
	}

	public static Coordinate toCoordinate(
			final Vector2D vec ) {
		return new Coordinate(
				vec.getX(),
				vec.getY());
	}

	public static Vector2D toVec(
			final Coordinate coor ) {
		return new Vector2D(
				coor.x,
				coor.y);
	}
}
