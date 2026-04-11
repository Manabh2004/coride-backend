from flask import Flask, request, jsonify
from flask_cors import CORS
import pymysql
import math

app = Flask(__name__)
CORS(app)

DB_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password': '',
    'database': 'coride',
    'cursorclass': pymysql.cursors.DictCursor
}

def get_db():
    return pymysql.connect(**DB_CONFIG)

def haversine(lat1, lng1, lat2, lng2):
    R = 6371
    d_lat = math.radians(lat2 - lat1)
    d_lng = math.radians(lng2 - lng1)
    a = (math.sin(d_lat/2)**2 +
         math.cos(math.radians(lat1)) *
         math.cos(math.radians(lat2)) *
         math.sin(d_lng/2)**2)
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))

def bayesian_rating(rating, count, global_avg=4.5, min_votes=5):
    return (count * rating + min_votes * global_avg) / (count + min_votes)

# ── USERS ───────────────────────────────────────────────

@app.route('/users/register', methods=['POST'])
def register_user():
    data = request.json
    db = get_db()
    try:
        with db.cursor() as cursor:
            cursor.execute(
                """INSERT INTO users (firebase_uid, name, email, phone)
                   VALUES (%s, %s, %s, %s)
                   ON DUPLICATE KEY UPDATE name=%s, phone=%s""",
                (data['firebase_uid'], data['name'], data['email'],
                 data['phone'], data['name'], data['phone'])
            )
        db.commit()
        return jsonify({'success': True})
    except Exception as e:
        return jsonify({'error': str(e)}), 400
    finally:
        db.close()

@app.route('/users/<firebase_uid>', methods=['GET'])
def get_user(firebase_uid):
    db = get_db()
    try:
        with db.cursor() as cursor:
            cursor.execute(
                'SELECT * FROM users WHERE firebase_uid=%s', (firebase_uid,)
            )
            user = cursor.fetchone()
        if not user:
            return jsonify({'error': 'User not found'}), 404
        return jsonify(user)
    finally:
        db.close()

# ── RIDES ───────────────────────────────────────────────

@app.route('/rides', methods=['POST'])
def create_ride():
    data = request.json
    db = get_db()
    try:
        with db.cursor() as cursor:
            cursor.execute(
                """INSERT INTO rides
                   (host_uid, host_name, origin_address, origin_lat, origin_lng,
                    destination_address, destination_lat, destination_lng,
                    departure_time, available_seats, total_seats, rate_per_km,
                    is_recurring, recurring_days)
                   VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                (
                    data['host_uid'], data['host_name'],
                    data['origin_address'], data['origin_lat'], data['origin_lng'],
                    data['destination_address'], data['destination_lat'], data['destination_lng'],
                    data['departure_time'], data['seats'], data['seats'],
                    data['rate_per_km'], data.get('is_recurring', False),
                    ','.join(data.get('recurring_days', []))
                )
            )
            ride_id = cursor.lastrowid
        db.commit()
        return jsonify({'success': True, 'ride_id': ride_id})
    except Exception as e:
        return jsonify({'error': str(e)}), 400
    finally:
        db.close()

@app.route('/rides/match', methods=['POST'])
def match_rides():
    data = request.json
    member_pickup_lat = data['pickup_lat']
    member_pickup_lng = data['pickup_lng']
    member_drop_lat = data['drop_lat']
    member_drop_lng = data['drop_lng']
    max_rate = data.get('max_rate', 999)
    member_uid = data.get('member_uid', '')

    db = get_db()
    try:
        with db.cursor() as cursor:
            cursor.execute(
                """SELECT r.*, u.rating, u.rating_count
                   FROM rides r
                   LEFT JOIN users u ON r.host_uid = u.firebase_uid
                   WHERE r.status='active'
                   AND r.available_seats > 0
                   AND r.rate_per_km <= %s
                   AND r.host_uid != %s""",
                (max_rate, member_uid)
            )
            rides = cursor.fetchall()

        results = []
        for ride in rides:
            dist = haversine(
                member_pickup_lat, member_pickup_lng,
                ride['origin_lat'], ride['origin_lng']
            )
            if dist > 30:
                continue

            pickup_to_origin = haversine(
                member_pickup_lat, member_pickup_lng,
                ride['origin_lat'], ride['origin_lng']
            )
            pickup_to_dest = haversine(
                member_pickup_lat, member_pickup_lng,
                ride['destination_lat'], ride['destination_lng']
            )
            host_route_len = haversine(
                ride['origin_lat'], ride['origin_lng'],
                ride['destination_lat'], ride['destination_lng']
            )

            detour = max(0, pickup_to_origin + pickup_to_dest - host_route_len)
            if detour > 3:
                continue

            overlap = max(0, 100 - (detour / host_route_len * 100)) if host_route_len > 0 else 50

            rating = ride['rating'] or 5.0
            count = ride['rating_count'] or 0
            b_rating = bayesian_rating(rating, count)
            rate_score = 100 - ((ride['rate_per_km'] / max_rate) * 100) if max_rate > 0 else 50
            score = (overlap * 0.40) + (b_rating / 5 * 100 * 0.20) + (rate_score * 0.15)

            results.append({
                'id': ride['id'],
                'hostName': ride['host_name'],
                'host_uid': ride['host_uid'],
                'origin': ride['origin_address'][:40],
                'destination': ride['destination_address'][:40],
                'origin_lat': ride['origin_lat'],
                'origin_lng': ride['origin_lng'],
                'time': ride['departure_time'],
                'seats': ride['available_seats'],
                'rate': ride['rate_per_km'],
                'rating': round(b_rating, 1),
                'overlap': round(overlap),
                'score': round(score, 1),
            })

        results.sort(key=lambda x: x['score'], reverse=True)
        return jsonify(results)

    except Exception as e:
        return jsonify({'error': str(e)}), 400
    finally:
        db.close()

@app.route('/rides/host/<host_uid>', methods=['GET'])
def get_host_rides(host_uid):
    db = get_db()
    try:
        with db.cursor() as cursor:
            cursor.execute(
                'SELECT * FROM rides WHERE host_uid=%s ORDER BY created_at DESC',
                (host_uid,)
            )
            rides = cursor.fetchall()
        return jsonify(rides)
    finally:
        db.close()

# ── BOOKINGS ─────────────────────────────────────────────

@app.route('/bookings', methods=['POST'])
def create_booking():
    data = request.json
    db = get_db()
    try:
        with db.cursor() as cursor:
            cursor.execute(
                """INSERT INTO bookings
                   (ride_id, member_uid, member_name,
                    pickup_address, pickup_lat, pickup_lng,
                    drop_address, drop_lat, drop_lng)
                   VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                (
                    data['ride_id'], data['member_uid'], data['member_name'],
                    data['pickup_address'], data['pickup_lat'], data['pickup_lng'],
                    data['drop_address'], data['drop_lat'], data['drop_lng'],
                )
            )
            cursor.execute(
                'UPDATE rides SET available_seats = available_seats - 1 WHERE id=%s',
                (data['ride_id'],)
            )
        db.commit()
        return jsonify({'success': True})
    except Exception as e:
        return jsonify({'error': str(e)}), 400
    finally:
        db.close()

@app.route('/bookings/member/<member_uid>', methods=['GET'])
def get_member_bookings(member_uid):
    db = get_db()
    try:
        with db.cursor() as cursor:
            cursor.execute(
                """SELECT b.*, r.departure_time, r.host_name,
                          r.origin_address, r.destination_address, r.rate_per_km
                   FROM bookings b
                   JOIN rides r ON b.ride_id = r.id
                   WHERE b.member_uid=%s
                   ORDER BY b.created_at DESC""",
                (member_uid,)
            )
            bookings = cursor.fetchall()
        return jsonify(bookings)
    finally:
        db.close()

@app.route('/bookings/ride/<int:ride_id>', methods=['GET'])
def get_ride_bookings(ride_id):
    db = get_db()
    try:
        with db.cursor() as cursor:
            cursor.execute(
                'SELECT * FROM bookings WHERE ride_id=%s', (ride_id,)
            )
            bookings = cursor.fetchall()
        return jsonify(bookings)
    finally:
        db.close()

@app.route('/bookings/<int:booking_id>/status', methods=['PUT'])
def update_booking_status(booking_id):
    data = request.json
    db = get_db()
    try:
        with db.cursor() as cursor:
            cursor.execute(
                'UPDATE bookings SET status=%s WHERE id=%s',
                (data['status'], booking_id)
            )
            if data['status'] == 'rejected':
                cursor.execute(
                    """UPDATE rides r
                       JOIN bookings b ON b.ride_id = r.id
                       SET r.available_seats = r.available_seats + 1
                       WHERE b.id=%s""",
                    (booking_id,)
                )
        db.commit()
        return jsonify({'success': True})
    except Exception as e:
        return jsonify({'error': str(e)}), 400
    finally:
        db.close()

# ── RATINGS ──────────────────────────────────────────────

@app.route('/ratings', methods=['POST'])
def submit_rating():
    data = request.json
    db = get_db()
    try:
        with db.cursor() as cursor:
            cursor.execute(
                'INSERT INTO ratings (from_uid, to_uid, ride_id, score) VALUES (%s,%s,%s,%s)',
                (data['from_uid'], data['to_uid'], data['ride_id'], data['score'])
            )
            cursor.execute(
                """UPDATE users
                   SET rating = ((rating * rating_count) + %s) / (rating_count + 1),
                       rating_count = rating_count + 1
                   WHERE firebase_uid = %s""",
                (data['score'], data['to_uid'])
            )
        db.commit()
        return jsonify({'success': True})
    except Exception as e:
        return jsonify({'error': str(e)}), 400
    finally:
        db.close()

# ── ECO STATS ─────────────────────────────────────────────

@app.route('/eco/<member_uid>', methods=['GET'])
def get_eco_stats(member_uid):
    db = get_db()
    try:
        with db.cursor() as cursor:
            cursor.execute(
                """SELECT COUNT(*) as total_rides,
                          SUM(
                            haversine_approx(
                              b.pickup_lat, b.pickup_lng,
                              b.drop_lat, b.drop_lng
                            )
                          ) as total_km
                   FROM bookings b
                   WHERE b.member_uid=%s AND b.status='accepted'""",
                (member_uid,)
            )
            row = cursor.fetchone()

        total_rides = row['total_rides'] or 0
        total_km = row['total_km'] or 0

        # CO2 calculation: 0.21 kg per km saved per passenger
        co2_saved = round(total_km * 0.21, 1)
        fuel_saved = round(co2_saved / 2.31, 1)
        trees = round(co2_saved / 21, 2)

        return jsonify({
            'total_rides': total_rides,
            'co2_saved_kg': co2_saved,
            'fuel_saved_litres': fuel_saved,
            'trees_equivalent': trees,
        })
    except Exception as e:
        # Return dummy stats if calculation fails
        return jsonify({
            'total_rides': 0,
            'co2_saved_kg': 0.0,
            'fuel_saved_litres': 0.0,
            'trees_equivalent': 0.0,
        })
    finally:
        db.close()

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)