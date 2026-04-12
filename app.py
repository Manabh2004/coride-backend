from flask import Flask, request, jsonify
from flask_cors import CORS
import sqlite3
import math
import os

app = Flask(__name__)
CORS(app)

DB_PATH = os.path.join(os.path.dirname(__file__), 'coride.db')

def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    db = get_db()
    db.executescript('''
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            firebase_uid TEXT UNIQUE NOT NULL,
            name TEXT NOT NULL,
            email TEXT UNIQUE NOT NULL,
            phone TEXT NOT NULL,
            rating REAL DEFAULT 5.0,
            rating_count INTEGER DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS rides (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            host_uid TEXT NOT NULL,
            host_name TEXT NOT NULL,
            origin_address TEXT NOT NULL,
            origin_lat REAL NOT NULL,
            origin_lng REAL NOT NULL,
            destination_address TEXT NOT NULL,
            destination_lat REAL NOT NULL,
            destination_lng REAL NOT NULL,
            departure_time TEXT NOT NULL,
            available_seats INTEGER NOT NULL,
            total_seats INTEGER NOT NULL,
            rate_per_km REAL NOT NULL,
            is_recurring INTEGER DEFAULT 0,
            recurring_days TEXT DEFAULT '',
            status TEXT DEFAULT 'active',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS bookings (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ride_id INTEGER NOT NULL,
            member_uid TEXT NOT NULL,
            member_name TEXT NOT NULL,
            pickup_address TEXT NOT NULL,
            pickup_lat REAL NOT NULL,
            pickup_lng REAL NOT NULL,
            drop_address TEXT NOT NULL,
            drop_lat REAL NOT NULL,
            drop_lng REAL NOT NULL,
            status TEXT DEFAULT 'pending',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS ratings (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            from_uid TEXT NOT NULL,
            to_uid TEXT NOT NULL,
            ride_id INTEGER NOT NULL,
            score REAL NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    ''')
    db.commit()
    db.close()

init_db()

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

def row_to_dict(row):
    return dict(zip(row.keys(), row))

# ── USERS ────────────────────────────────────────────────

@app.route('/users/register', methods=['POST'])
def register_user():
    data = request.json
    db = get_db()
    try:
        db.execute(
            '''INSERT INTO users (firebase_uid, name, email, phone)
               VALUES (?, ?, ?, ?)
               ON CONFLICT(firebase_uid) DO UPDATE SET name=excluded.name, phone=excluded.phone''',
            (data['firebase_uid'], data['name'], data['email'], data['phone'])
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
        row = db.execute('SELECT * FROM users WHERE firebase_uid=?', (firebase_uid,)).fetchone()
        if not row:
            return jsonify({'error': 'User not found'}), 404
        return jsonify(row_to_dict(row))
    finally:
        db.close()

# ── RIDES ─────────────────────────────────────────────────

@app.route('/rides', methods=['POST'])
def create_ride():
    data = request.json
    db = get_db()
    try:
        cursor = db.execute(
            '''INSERT INTO rides
               (host_uid, host_name, origin_address, origin_lat, origin_lng,
                destination_address, destination_lat, destination_lng,
                departure_time, available_seats, total_seats, rate_per_km,
                is_recurring, recurring_days)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)''',
            (
                data['host_uid'], data['host_name'],
                data['origin_address'], data['origin_lat'], data['origin_lng'],
                data['destination_address'], data['destination_lat'], data['destination_lng'],
                data['departure_time'], data['seats'], data['seats'],
                data['rate_per_km'], 1 if data.get('is_recurring') else 0,
                ','.join(data.get('recurring_days', []))
            )
        )
        db.commit()
        return jsonify({'success': True, 'ride_id': cursor.lastrowid})
    except Exception as e:
        return jsonify({'error': str(e)}), 400
    finally:
        db.close()

@app.route('/rides/match', methods=['POST'])
def match_rides():
    data = request.json
    pickup_lat = data['pickup_lat']
    pickup_lng = data['pickup_lng']
    drop_lat = data['drop_lat']
    drop_lng = data['drop_lng']
    max_rate = data.get('max_rate', 999)
    member_uid = data.get('member_uid', '')

    db = get_db()
    try:
        rows = db.execute(
            '''SELECT r.*, u.rating, u.rating_count
               FROM rides r
               LEFT JOIN users u ON r.host_uid = u.firebase_uid
               WHERE r.status='active'
               AND r.available_seats > 0
               AND r.rate_per_km <= ?
               AND r.host_uid != ?''',
            (max_rate, member_uid)
        ).fetchall()

        results = []
        for row in rows:
            ride = row_to_dict(row)
            dist = haversine(pickup_lat, pickup_lng, ride['origin_lat'], ride['origin_lng'])
            if dist > 30:
                continue

            p2o = haversine(pickup_lat, pickup_lng, ride['origin_lat'], ride['origin_lng'])
            p2d = haversine(pickup_lat, pickup_lng, ride['destination_lat'], ride['destination_lng'])
            route_len = haversine(ride['origin_lat'], ride['origin_lng'], ride['destination_lat'], ride['destination_lng'])

            detour = max(0, p2o + p2d - route_len)
            if detour > 3:
                continue

            overlap = max(0, 100 - (detour / route_len * 100)) if route_len > 0 else 50
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
        rows = db.execute(
            'SELECT * FROM rides WHERE host_uid=? ORDER BY created_at DESC', (host_uid,)
        ).fetchall()
        return jsonify([row_to_dict(r) for r in rows])
    finally:
        db.close()

# ── BOOKINGS ──────────────────────────────────────────────

@app.route('/bookings', methods=['POST'])
def create_booking():
    data = request.json
    db = get_db()
    try:
        db.execute(
            '''INSERT INTO bookings
               (ride_id, member_uid, member_name,
                pickup_address, pickup_lat, pickup_lng,
                drop_address, drop_lat, drop_lng)
               VALUES (?,?,?,?,?,?,?,?,?)''',
            (data['ride_id'], data['member_uid'], data['member_name'],
             data['pickup_address'], data['pickup_lat'], data['pickup_lng'],
             data['drop_address'], data['drop_lat'], data['drop_lng'])
        )
        db.execute('UPDATE rides SET available_seats = available_seats - 1 WHERE id=?', (data['ride_id'],))
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
        rows = db.execute(
            '''SELECT b.*, r.departure_time, r.host_name,
                      r.origin_address, r.destination_address, r.rate_per_km
               FROM bookings b
               JOIN rides r ON b.ride_id = r.id
               WHERE b.member_uid=?
               ORDER BY b.created_at DESC''',
            (member_uid,)
        ).fetchall()
        return jsonify([row_to_dict(r) for r in rows])
    finally:
        db.close()

@app.route('/bookings/ride/<int:ride_id>', methods=['GET'])
def get_ride_bookings(ride_id):
    db = get_db()
    try:
        rows = db.execute(
            'SELECT * FROM bookings WHERE ride_id=?', (ride_id,)
        ).fetchall()
        return jsonify([row_to_dict(r) for r in rows])
    finally:
        db.close()

@app.route('/bookings/<int:booking_id>/status', methods=['PUT'])
def update_booking_status(booking_id):
    data = request.json
    db = get_db()
    try:
        db.execute('UPDATE bookings SET status=? WHERE id=?', (data['status'], booking_id))
        if data['status'] == 'rejected':
            row = db.execute('SELECT ride_id FROM bookings WHERE id=?', (booking_id,)).fetchone()
            if row:
                db.execute('UPDATE rides SET available_seats = available_seats + 1 WHERE id=?', (row['ride_id'],))
        db.commit()
        return jsonify({'success': True})
    except Exception as e:
        return jsonify({'error': str(e)}), 400
    finally:
        db.close()

# ── RATINGS ───────────────────────────────────────────────

@app.route('/ratings', methods=['POST'])
def submit_rating():
    data = request.json
    db = get_db()
    try:
        db.execute(
            'INSERT INTO ratings (from_uid, to_uid, ride_id, score) VALUES (?,?,?,?)',
            (data['from_uid'], data['to_uid'], data['ride_id'], data['score'])
        )
        db.execute(
            '''UPDATE users
               SET rating = ((rating * rating_count) + ?) / (rating_count + 1),
                   rating_count = rating_count + 1
               WHERE firebase_uid = ?''',
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
        rows = db.execute(
            '''SELECT pickup_lat, pickup_lng, drop_lat, drop_lng
               FROM bookings
               WHERE member_uid=? AND status='accepted' ''',
            (member_uid,)
        ).fetchall()

        total_km = 0
        for row in rows:
            r = row_to_dict(row)
            total_km += haversine(r['pickup_lat'], r['pickup_lng'], r['drop_lat'], r['drop_lng'])

        co2_saved = round(total_km * 0.21, 1)
        fuel_saved = round(co2_saved / 2.31, 1)
        trees = round(co2_saved / 21, 2)

        return jsonify({
            'total_rides': len(rows),
            'co2_saved_kg': co2_saved,
            'fuel_saved_litres': fuel_saved,
            'trees_equivalent': trees,
        })
    except Exception as e:
        return jsonify({'total_rides': 0, 'co2_saved_kg': 0.0, 'fuel_saved_litres': 0.0, 'trees_equivalent': 0.0})
    finally:
        db.close()

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)