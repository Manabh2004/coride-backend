from flask import Flask, request, jsonify
from flask_cors import CORS
import sqlite3, math, os, json, urllib.request

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
            vouch_count INTEGER DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS push_tokens (
            firebase_uid TEXT PRIMARY KEY,
            push_token TEXT NOT NULL,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
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
            ride_date TEXT NOT NULL DEFAULT '',
            available_seats INTEGER NOT NULL,
            total_seats INTEGER NOT NULL,
            rate_per_km REAL NOT NULL,
            is_recurring INTEGER DEFAULT 0,
            recurring_days TEXT DEFAULT '',
            auto_accept INTEGER DEFAULT 0,
            min_rating_required REAL DEFAULT 0.0,
            min_vouches_required INTEGER DEFAULT 0,
            require_network_vouch INTEGER DEFAULT 0,
            status TEXT DEFAULT 'active',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS member_searches (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            member_uid TEXT NOT NULL,
            member_name TEXT NOT NULL,
            member_rating REAL DEFAULT 5.0,
            member_vouch_count INTEGER DEFAULT 0,
            pickup_address TEXT NOT NULL,
            pickup_lat REAL NOT NULL,
            pickup_lng REAL NOT NULL,
            drop_address TEXT NOT NULL,
            drop_lat REAL NOT NULL,
            drop_lng REAL NOT NULL,
            departure_time TEXT NOT NULL,
            ride_date TEXT NOT NULL DEFAULT '',
            max_rate REAL NOT NULL,
            status TEXT DEFAULT 'searching',
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
        CREATE TABLE IF NOT EXISTS vouches (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            from_uid TEXT NOT NULL,
            to_uid TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(from_uid, to_uid)
        );
        CREATE TABLE IF NOT EXISTS live_locations (
            ride_id TEXT PRIMARY KEY,
            lat REAL NOT NULL,
            lng REAL NOT NULL,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    ''')
    for col in [
        ('rides', 'ride_date', 'TEXT DEFAULT ""'),
        ('member_searches', 'ride_date', 'TEXT DEFAULT ""'),
    ]:
        try:
            db.execute(f'ALTER TABLE {col[0]} ADD COLUMN {col[1]} {col[2]}')
            db.commit()
        except: pass
    db.close()

init_db()

def haversine(lat1, lng1, lat2, lng2):
    R = 6371
    d_lat = math.radians(lat2 - lat1)
    d_lng = math.radians(lng2 - lng1)
    a = (math.sin(d_lat/2)**2 +
         math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) *
         math.sin(d_lng/2)**2)
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))

def bayesian_rating(rating, count, global_avg=4.5, min_votes=5):
    return (count * rating + min_votes * global_avg) / (count + min_votes)

def row_to_dict(row):
    return dict(zip(row.keys(), row))

def get_push_token(firebase_uid):
    db = get_db()
    try:
        row = db.execute('SELECT push_token FROM push_tokens WHERE firebase_uid=?', (firebase_uid,)).fetchone()
        return row['push_token'] if row else None
    finally: db.close()

def send_push_notification(push_token, title, body, data=None):
    if not push_token: return
    try:
        payload = json.dumps({
            'to': push_token, 'title': title, 'body': body,
            'data': data or {}, 'sound': 'default', 'priority': 'high',
        }).encode('utf-8')
        req = urllib.request.Request(
            'https://exp.host/--/api/v2/push/send', data=payload,
            headers={'Content-Type': 'application/json', 'Accept': 'application/json'}
        )
        urllib.request.urlopen(req, timeout=5)
    except Exception as e:
        print(f'Push failed: {e}')

@app.route('/ping', methods=['GET'])
def ping():
    return jsonify({'status': 'alive'})

@app.route('/track/live', methods=['GET'])
def track_live():
    lat = request.args.get('lat', '20.2961')
    lng = request.args.get('lng', '85.8245')
    ride_id = request.args.get('ride', '')
    if ride_id:
        db = get_db()
        try:
            row = db.execute('SELECT lat, lng FROM live_locations WHERE ride_id=?', (ride_id,)).fetchone()
            if row: lat, lng = str(row['lat']), str(row['lng'])
        except: pass
        finally: db.close()
    html = f'''<!DOCTYPE html><html>
<head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>CoRide Live</title>
<link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css"/>
<script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
<style>*{{margin:0;padding:0}}body{{font-family:sans-serif;background:#1a1a1a}}
#h{{background:#1a1a1a;padding:12px 16px;display:flex;justify-content:space-between;align-items:center}}
#h h1{{color:#F5C842;font-size:15px}}#st{{color:#aaa;font-size:11px}}
#map{{height:calc(100vh - 44px)}}
@keyframes p{{0%{{box-shadow:0 0 0 0 rgba(231,76,60,.6)}}70%{{box-shadow:0 0 0 12px rgba(231,76,60,0)}}100%{{box-shadow:0 0 0 0 rgba(231,76,60,0)}}}}
.d{{width:16px;height:16px;background:#e74c3c;border:3px solid #fff;border-radius:50%;animation:p 2s infinite}}</style>
</head><body>
<div id="h"><h1>🚗 CoRide Live Tracker</h1><span id="st">Connecting...</span></div>
<div id="map"></div>
<script>
var rid='{ride_id}',map=L.map('map').setView([{lat},{lng}],15),last=Date.now();
L.tileLayer('https://{{s}}.tile.openstreetmap.org/{{z}}/{{x}}/{{y}}.png',{{attribution:'© OpenStreetMap'}}).addTo(map);
var mk=L.marker([{lat},{lng}],{{icon:L.divIcon({{html:'<div class="d"></div>',iconSize:[16,16],iconAnchor:[8,8],className:''}})
}}).addTo(map).bindPopup('Live location').openPopup();
var st=document.getElementById('st');
function upd(){{fetch('/location/current/'+rid).then(r=>r.json()).then(d=>{{
if(d.lat&&d.lng){{mk.setLatLng([d.lat,d.lng]);map.panTo([d.lat,d.lng]);last=Date.now();st.textContent='Live';}}
}}).catch(()=>{{}});}}
setInterval(upd,15000);setInterval(()=>{{st.textContent=Math.round((Date.now()-last)/1000)+'s ago';}},1000);upd();
</script></body></html>'''
    return html, 200, {'Content-Type': 'text/html'}

@app.route('/location/update', methods=['POST'])
def update_location():
    data = request.json
    db = get_db()
    try:
        db.execute(
            '''INSERT INTO live_locations (ride_id, lat, lng) VALUES (?,?,?)
               ON CONFLICT(ride_id) DO UPDATE SET lat=excluded.lat,lng=excluded.lng,updated_at=CURRENT_TIMESTAMP''',
            (str(data['ride_id']), data['lat'], data['lng'])
        )
        db.commit()
        return jsonify({'success': True})
    except Exception as e:
        return jsonify({'error': str(e)}), 400
    finally: db.close()

@app.route('/location/current/<ride_id>', methods=['GET'])
def get_current_location(ride_id):
    db = get_db()
    try:
        row = db.execute('SELECT lat,lng,updated_at FROM live_locations WHERE ride_id=?', (str(ride_id),)).fetchone()
        if not row: return jsonify({'error': 'No data'}), 404
        return jsonify({'lat': row['lat'], 'lng': row['lng'], 'updated_at': row['updated_at']})
    finally: db.close()

@app.route('/users/register', methods=['POST'])
def register_user():
    data = request.json
    db = get_db()
    try:
        db.execute(
            '''INSERT INTO users (firebase_uid,name,email,phone) VALUES (?,?,?,?)
               ON CONFLICT(firebase_uid) DO UPDATE SET name=excluded.name,phone=excluded.phone''',
            (data['firebase_uid'], data['name'], data['email'], data['phone'])
        )
        db.commit()
        return jsonify({'success': True})
    except Exception as e:
        return jsonify({'error': str(e)}), 400
    finally: db.close()

@app.route('/users/<firebase_uid>', methods=['GET'])
def get_user(firebase_uid):
    db = get_db()
    try:
        row = db.execute('SELECT * FROM users WHERE firebase_uid=?', (firebase_uid,)).fetchone()
        if not row: return jsonify({'error': 'Not found'}), 404
        return jsonify(row_to_dict(row))
    finally: db.close()

@app.route('/users/register-token', methods=['POST'])
def register_push_token():
    data = request.json
    db = get_db()
    try:
        db.execute(
            '''INSERT INTO push_tokens (firebase_uid,push_token) VALUES (?,?)
               ON CONFLICT(firebase_uid) DO UPDATE SET push_token=excluded.push_token,updated_at=CURRENT_TIMESTAMP''',
            (data['firebase_uid'], data['push_token'])
        )
        db.commit()
        return jsonify({'success': True})
    except Exception as e:
        return jsonify({'error': str(e)}), 400
    finally: db.close()

@app.route('/vouches', methods=['POST'])
def add_vouch():
    data = request.json
    from_uid, to_uid = data['from_uid'], data['to_uid']
    if from_uid == to_uid: return jsonify({'error': 'Cannot vouch for yourself'}), 400
    db = get_db()
    try:
        existing = db.execute('SELECT id FROM vouches WHERE from_uid=? AND to_uid=?', (from_uid, to_uid)).fetchone()
        if existing:
            db.execute('DELETE FROM vouches WHERE from_uid=? AND to_uid=?', (from_uid, to_uid))
            db.execute('UPDATE users SET vouch_count=MAX(0,vouch_count-1) WHERE firebase_uid=?', (to_uid,))
            db.commit()
            return jsonify({'success': True, 'action': 'removed'})
        else:
            db.execute('INSERT INTO vouches (from_uid,to_uid) VALUES (?,?)', (from_uid, to_uid))
            db.execute('UPDATE users SET vouch_count=vouch_count+1 WHERE firebase_uid=?', (to_uid,))
            db.commit()
            return jsonify({'success': True, 'action': 'added'})
    except Exception as e:
        return jsonify({'error': str(e)}), 400
    finally: db.close()

@app.route('/vouches/details/<target_uid>', methods=['GET'])
def get_vouch_details(target_uid):
    viewer_uid = request.args.get('viewer_uid', '')
    db = get_db()
    try:
        user = db.execute('SELECT vouch_count FROM users WHERE firebase_uid=?', (target_uid,)).fetchone()
        total = user['vouch_count'] if user else 0
        direct = db.execute('SELECT id FROM vouches WHERE from_uid=? AND to_uid=?', (viewer_uid, target_uid)).fetchone()
        network_rows = db.execute(
            '''SELECT u.name,u.firebase_uid FROM vouches v1
               JOIN vouches v2 ON v1.to_uid=v2.from_uid
               JOIN users u ON u.firebase_uid=v1.to_uid
               WHERE v1.from_uid=? AND v2.to_uid=? LIMIT 5''',
            (viewer_uid, target_uid)
        ).fetchall()
        network = [row_to_dict(r) for r in network_rows]
        return jsonify({
            'total_vouches': total, 'viewer_vouched': direct is not None,
            'network_vouchers': network, 'network_count': len(network),
        })
    finally: db.close()

@app.route('/rides', methods=['POST'])
def create_ride():
    data = request.json
    db = get_db()
    try:
        cursor = db.execute(
            '''INSERT INTO rides
               (host_uid,host_name,origin_address,origin_lat,origin_lng,
                destination_address,destination_lat,destination_lng,
                departure_time,ride_date,available_seats,total_seats,rate_per_km,
                is_recurring,recurring_days,auto_accept,
                min_rating_required,min_vouches_required,require_network_vouch)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)''',
            (
                data['host_uid'], data['host_name'],
                data['origin_address'], data['origin_lat'], data['origin_lng'],
                data['destination_address'], data['destination_lat'], data['destination_lng'],
                data['departure_time'], data.get('ride_date', ''),
                data['seats'], data['seats'], data['rate_per_km'],
                1 if data.get('is_recurring') else 0,
                ','.join(data.get('recurring_days', [])),
                1 if data.get('auto_accept') else 0,
                data.get('min_rating_required', 0.0),
                data.get('min_vouches_required', 0),
                1 if data.get('require_network_vouch') else 0,
            )
        )
        db.commit()
        return jsonify({'success': True, 'ride_id': cursor.lastrowid})
    except Exception as e:
        return jsonify({'error': str(e)}), 400
    finally: db.close()

@app.route('/rides/<int:ride_id>/end', methods=['PUT'])
def end_ride(ride_id):
    db = get_db()
    try:
        db.execute("UPDATE rides SET status='completed' WHERE id=?", (ride_id,))
        db.commit()
        # Notify all accepted members
        bookings = db.execute(
            "SELECT member_uid FROM bookings WHERE ride_id=? AND status='accepted'", (ride_id,)
        ).fetchall()
        for b in bookings:
            token = get_push_token(b['member_uid'])
            send_push_notification(token, '🏁 Ride ended', 'Your host has ended the ride. Please rate your experience.', {'screen': 'MyBookings'})
        return jsonify({'success': True})
    except Exception as e:
        return jsonify({'error': str(e)}), 400
    finally: db.close()

@app.route('/rides/match', methods=['POST'])
def match_rides():
    data = request.json
    pickup_lat = data['pickup_lat']
    pickup_lng = data['pickup_lng']
    drop_lat = data['drop_lat']
    drop_lng = data['drop_lng']
    max_rate = data.get('max_rate', 999)
    member_uid = data.get('member_uid', '')
    ride_date = data.get('ride_date', '')

    db = get_db()
    try:
        rows = db.execute(
            '''SELECT r.*, u.rating, u.rating_count, u.vouch_count
               FROM rides r
               LEFT JOIN users u ON r.host_uid = u.firebase_uid
               WHERE r.status='active'
               AND r.available_seats > 0
               AND r.rate_per_km <= ?
               AND r.host_uid != ?
               AND (r.ride_date = ? OR r.ride_date = '')
               AND r.id NOT IN (
                   SELECT ride_id FROM bookings
                   WHERE member_uid=? AND status IN ('pending','accepted')
               )''',
            (max_rate, member_uid, ride_date, member_uid)
        ).fetchall()

        results = []
        for row in rows:
            ride = row_to_dict(row)
            dist = haversine(pickup_lat, pickup_lng, ride['origin_lat'], ride['origin_lng'])
            if dist > 30: continue
            p2o = haversine(pickup_lat, pickup_lng, ride['origin_lat'], ride['origin_lng'])
            p2d = haversine(pickup_lat, pickup_lng, ride['destination_lat'], ride['destination_lng'])
            route_len = haversine(ride['origin_lat'], ride['origin_lng'], ride['destination_lat'], ride['destination_lng'])
            detour = max(0, p2o + p2d - route_len)
            if detour > 3: continue
            overlap = max(0, 100 - (detour / route_len * 100)) if route_len > 0 else 50
            rating = ride['rating'] or 5.0
            count = ride['rating_count'] or 0
            b_rating = bayesian_rating(rating, count)
            rate_score = 100 - ((ride['rate_per_km'] / max_rate) * 100) if max_rate > 0 else 50
            score = (overlap * 0.40) + (b_rating / 5 * 100 * 0.20) + (rate_score * 0.15)
            results.append({
                'id': ride['id'], 'hostName': ride['host_name'], 'host_uid': ride['host_uid'],
                'origin': ride['origin_address'][:40], 'destination': ride['destination_address'][:40],
                'origin_lat': ride['origin_lat'], 'origin_lng': ride['origin_lng'],
                'time': ride['departure_time'], 'ride_date': ride['ride_date'],
                'seats': ride['available_seats'], 'rate': ride['rate_per_km'],
                'rating': round(b_rating, 1), 'vouch_count': ride['vouch_count'] or 0,
                'overlap': round(overlap), 'score': round(score, 1),
                'auto_accept': bool(ride['auto_accept']), 'detour_km': round(detour, 1),
            })
        results.sort(key=lambda x: x['score'], reverse=True)
        return jsonify(results)
    except Exception as e:
        return jsonify({'error': str(e)}), 400
    finally: db.close()

@app.route('/rides/host/<host_uid>', methods=['GET'])
def get_host_rides(host_uid):
    db = get_db()
    try:
        rows = db.execute(
            "SELECT * FROM rides WHERE host_uid=? AND status='active' ORDER BY created_at DESC",
            (host_uid,)
        ).fetchall()
        return jsonify([row_to_dict(r) for r in rows])
    finally: db.close()

@app.route('/searches', methods=['POST'])
def post_search():
    data = request.json
    db = get_db()
    try:
        user = db.execute('SELECT rating,rating_count,vouch_count FROM users WHERE firebase_uid=?', (data['member_uid'],)).fetchone()
        rating = user['rating'] if user else 5.0
        b_rating = bayesian_rating(rating, user['rating_count'] if user else 0)
        vouch_count = user['vouch_count'] if user else 0
        db.execute('DELETE FROM member_searches WHERE member_uid=?', (data['member_uid'],))
        cursor = db.execute(
            '''INSERT INTO member_searches
               (member_uid,member_name,member_rating,member_vouch_count,
                pickup_address,pickup_lat,pickup_lng,drop_address,drop_lat,drop_lng,
                departure_time,ride_date,max_rate)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)''',
            (
                data['member_uid'], data['member_name'], round(b_rating, 1), vouch_count,
                data['pickup_address'], data['pickup_lat'], data['pickup_lng'],
                data['drop_address'], data['drop_lat'], data['drop_lng'],
                data['departure_time'], data.get('ride_date', ''), data['max_rate'],
            )
        )
        db.commit()
        return jsonify({'success': True, 'search_id': cursor.lastrowid})
    except Exception as e:
        return jsonify({'error': str(e)}), 400
    finally: db.close()

@app.route('/searches/match/<int:ride_id>', methods=['GET'])
def get_matching_members(ride_id):
    db = get_db()
    try:
        ride = db.execute('SELECT * FROM rides WHERE id=?', (ride_id,)).fetchone()
        if not ride: return jsonify({'error': 'Not found'}), 404
        ride = row_to_dict(ride)
        searches = db.execute(
            '''SELECT * FROM member_searches WHERE status='searching'
               AND member_uid != ?
               AND (ride_date = ? OR ride_date = '')
               AND member_uid NOT IN (
                   SELECT member_uid FROM bookings
                   WHERE ride_id=? AND status IN ('pending','accepted')
               )''',
            (ride['host_uid'], ride.get('ride_date', ''), ride_id)
        ).fetchall()
        results = []
        for row in searches:
            s = row_to_dict(row)
            p2o = haversine(s['pickup_lat'], s['pickup_lng'], ride['origin_lat'], ride['origin_lng'])
            p2d = haversine(s['pickup_lat'], s['pickup_lng'], ride['destination_lat'], ride['destination_lng'])
            route_len = haversine(ride['origin_lat'], ride['origin_lng'], ride['destination_lat'], ride['destination_lng'])
            detour = max(0, p2o + p2d - route_len)
            if detour > 5: continue
            if ride['rate_per_km'] > s['max_rate']: continue
            results.append({
                'id': s['id'], 'member_uid': s['member_uid'], 'memberName': s['member_name'],
                'rating': s['member_rating'], 'vouch_count': s['member_vouch_count'],
                'pickup_address': s['pickup_address'], 'pickup_lat': s['pickup_lat'],
                'pickup_lng': s['pickup_lng'], 'drop_address': s['drop_address'],
                'drop_lat': s['drop_lat'], 'drop_lng': s['drop_lng'],
                'time': s['departure_time'], 'max_rate': s['max_rate'], 'detour_km': round(detour, 1),
            })
        results.sort(key=lambda x: x['rating'], reverse=True)
        return jsonify(results)
    except Exception as e:
        return jsonify({'error': str(e)}), 400
    finally: db.close()

@app.route('/bookings', methods=['POST'])
def create_booking():
    data = request.json
    db = get_db()
    try:
        existing = db.execute(
            "SELECT id FROM bookings WHERE ride_id=? AND member_uid=? AND status IN ('pending','accepted')",
            (data['ride_id'], data['member_uid'])
        ).fetchone()
        if existing: return jsonify({'error': 'Already booked'}), 400

        ride = db.execute('SELECT * FROM rides WHERE id=?', (data['ride_id'],)).fetchone()
        if not ride or ride['available_seats'] <= 0: return jsonify({'error': 'No seats'}), 400
        ride = row_to_dict(ride)

        status = 'pending'
        if ride['auto_accept']:
            member = db.execute('SELECT rating,rating_count,vouch_count FROM users WHERE firebase_uid=?', (data['member_uid'],)).fetchone()
            if member:
                b_rating = bayesian_rating(member['rating'], member['rating_count'])
                meets = b_rating >= ride['min_rating_required'] and member['vouch_count'] >= ride['min_vouches_required']
                if meets and ride['require_network_vouch']:
                    net = db.execute(
                        '''SELECT v2.from_uid FROM vouches v1 JOIN vouches v2 ON v1.to_uid=v2.from_uid
                           WHERE v1.from_uid=? AND v2.to_uid=? LIMIT 1''',
                        (ride['host_uid'], data['member_uid'])
                    ).fetchone()
                    direct = db.execute('SELECT id FROM vouches WHERE from_uid=? AND to_uid=?', (ride['host_uid'], data['member_uid'])).fetchone()
                    meets = meets and ((net is not None) or (direct is not None))
                if meets: status = 'accepted'

        cursor = db.execute(
            '''INSERT INTO bookings (ride_id,member_uid,member_name,pickup_address,pickup_lat,
               pickup_lng,drop_address,drop_lat,drop_lng,status)
               VALUES (?,?,?,?,?,?,?,?,?,?)''',
            (data['ride_id'], data['member_uid'], data['member_name'],
             data['pickup_address'], data['pickup_lat'], data['pickup_lng'],
             data['drop_address'], data['drop_lat'], data['drop_lng'], status)
        )
        if status == 'accepted':
            db.execute('UPDATE rides SET available_seats=available_seats-1 WHERE id=?', (data['ride_id'],))
        db.execute("UPDATE member_searches SET status='matched' WHERE member_uid=?", (data['member_uid'],))
        db.commit()

        host_token = get_push_token(ride['host_uid'])
        if status == 'accepted':
            send_push_notification(host_token, '✅ Rider auto-accepted', f'{data["member_name"]} joined your ride.', {'screen': 'HostDashboard'})
        else:
            send_push_notification(host_token, '🚗 New ride request', f'{data["member_name"]} wants to join. Tap to review.', {'screen': 'HostDashboard'})

        return jsonify({'success': True, 'status': status, 'booking_id': cursor.lastrowid})
    except Exception as e:
        return jsonify({'error': str(e)}), 400
    finally: db.close()

@app.route('/bookings/host-invite', methods=['POST'])
def host_invite_member():
    data = request.json
    db = get_db()
    try:
        existing = db.execute(
            "SELECT id FROM bookings WHERE ride_id=? AND member_uid=? AND status IN ('pending','accepted')",
            (data['ride_id'], data['member_uid'])
        ).fetchone()
        if existing: return jsonify({'error': 'Already booked'}), 400

        ride = db.execute('SELECT available_seats,host_uid FROM rides WHERE id=?', (data['ride_id'],)).fetchone()
        if not ride or ride['available_seats'] <= 0: return jsonify({'error': 'No seats'}), 400

        db.execute(
            '''INSERT INTO bookings (ride_id,member_uid,member_name,pickup_address,pickup_lat,
               pickup_lng,drop_address,drop_lat,drop_lng,status) VALUES (?,?,?,?,?,?,?,?,?,'accepted')''',
            (data['ride_id'], data['member_uid'], data['member_name'],
             data['pickup_address'], data['pickup_lat'], data['pickup_lng'],
             data['drop_address'], data['drop_lat'], data['drop_lng'])
        )
        db.execute('UPDATE rides SET available_seats=available_seats-1 WHERE id=?', (data['ride_id'],))
        db.execute("UPDATE member_searches SET status='matched' WHERE member_uid=?", (data['member_uid'],))
        db.commit()

        member_token = get_push_token(data['member_uid'])
        send_push_notification(member_token, '🎉 You got picked!', 'A host has added you to their ride. Check My Bookings.', {'screen': 'MyBookings'})

        return jsonify({'success': True})
    except Exception as e:
        return jsonify({'error': str(e)}), 400
    finally: db.close()

@app.route('/bookings/member/<member_uid>', methods=['GET'])
def get_member_bookings(member_uid):
    db = get_db()
    try:
        rows = db.execute(
            '''SELECT b.*, r.departure_time, r.host_name, r.origin_address,
                      r.destination_address, r.rate_per_km, r.host_uid,
                      r.origin_lat, r.origin_lng, r.id as ride_id_ref
               FROM bookings b JOIN rides r ON b.ride_id=r.id
               WHERE b.member_uid=? ORDER BY b.created_at DESC''',
            (member_uid,)
        ).fetchall()
        return jsonify([row_to_dict(r) for r in rows])
    finally: db.close()

@app.route('/bookings/ride/<int:ride_id>', methods=['GET'])
def get_ride_bookings(ride_id):
    db = get_db()
    try:
        rows = db.execute('SELECT * FROM bookings WHERE ride_id=?', (ride_id,)).fetchall()
        return jsonify([row_to_dict(r) for r in rows])
    finally: db.close()

@app.route('/bookings/<int:booking_id>/status', methods=['PUT'])
def update_booking_status(booking_id):
    data = request.json
    db = get_db()
    try:
        booking = db.execute('SELECT ride_id,member_uid,member_name FROM bookings WHERE id=?', (booking_id,)).fetchone()
        db.execute('UPDATE bookings SET status=? WHERE id=?', (data['status'], booking_id))
        if data['status'] == 'accepted' and booking:
            db.execute('UPDATE rides SET available_seats=available_seats-1 WHERE id=?', (booking['ride_id'],))
        elif data['status'] == 'rejected' and booking:
            db.execute('UPDATE rides SET available_seats=available_seats+1 WHERE id=?', (booking['ride_id'],))
        db.commit()
        if booking:
            token = get_push_token(booking['member_uid'])
            if data['status'] == 'accepted':
                send_push_notification(token, '✅ Ride confirmed!', 'Your booking was accepted. Have a safe trip!', {'screen': 'MyBookings'})
            elif data['status'] == 'rejected':
                send_push_notification(token, '❌ Booking declined', 'The host declined your request. Try another ride.', {'screen': 'MemberDashboard'})
        return jsonify({'success': True})
    except Exception as e:
        return jsonify({'error': str(e)}), 400
    finally: db.close()

@app.route('/ratings', methods=['POST'])
def submit_rating():
    data = request.json
    db = get_db()
    try:
        db.execute('INSERT INTO ratings (from_uid,to_uid,ride_id,score) VALUES (?,?,?,?)',
                   (data['from_uid'], data['to_uid'], data['ride_id'], data['score']))
        db.execute(
            'UPDATE users SET rating=((rating*rating_count)+?)/(rating_count+1),rating_count=rating_count+1 WHERE firebase_uid=?',
            (data['score'], data['to_uid'])
        )
        db.commit()
        return jsonify({'success': True})
    except Exception as e:
        return jsonify({'error': str(e)}), 400
    finally: db.close()

@app.route('/ratings/<firebase_uid>', methods=['GET'])
def get_user_ratings(firebase_uid):
    db = get_db()
    try:
        user = db.execute('SELECT rating,rating_count FROM users WHERE firebase_uid=?', (firebase_uid,)).fetchone()
        ratings = db.execute(
            '''SELECT r.score,r.created_at,u.name as from_name
               FROM ratings r JOIN users u ON r.from_uid=u.firebase_uid
               WHERE r.to_uid=? ORDER BY r.created_at DESC LIMIT 10''',
            (firebase_uid,)
        ).fetchall()
        return jsonify({
            'avg_rating': round(user['rating'], 1) if user else 5.0,
            'rating_count': user['rating_count'] if user else 0,
            'recent': [row_to_dict(r) for r in ratings],
        })
    finally: db.close()

@app.route('/eco/<member_uid>', methods=['GET'])
def get_eco_stats(member_uid):
    db = get_db()
    try:
        rows = db.execute(
            "SELECT pickup_lat,pickup_lng,drop_lat,drop_lng FROM bookings WHERE member_uid=? AND status='accepted'",
            (member_uid,)
        ).fetchall()
        total_km = sum(haversine(r['pickup_lat'],r['pickup_lng'],r['drop_lat'],r['drop_lng']) for r in rows)
        co2 = round(total_km * 0.21, 1)
        fuel = round(co2 / 2.31, 1)
        return jsonify({'total_rides': len(rows), 'co2_saved_kg': co2, 'fuel_saved_litres': fuel, 'trees_equivalent': round(co2/21, 2)})
    except:
        return jsonify({'total_rides': 0, 'co2_saved_kg': 0.0, 'fuel_saved_litres': 0.0, 'trees_equivalent': 0.0})
    finally: db.close()

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)