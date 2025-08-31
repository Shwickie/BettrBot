# Add this diagnostic function to check your model's calibration

def diagnose_model_issues(self, conn):
    """
    Check for common model problems that cause unrealistic edges
    """
    print("=== MODEL DIAGNOSTICS ===")
    
    # 1. Check probability distribution
    recent_games = pd.read_sql_query("""
        SELECT game_id, home_team, away_team, game_date
        FROM games 
        WHERE game_date >= date('now', '-30 days')
        ORDER BY game_date DESC
        LIMIT 20
    """, conn)
    
    probs = []
    for _, game in recent_games.iterrows():
        game_dict = {
            'game_id': game['game_id'],
            'home': game['home_team'], 
            'away': game['away_team'],
            'date': game['game_date']
        }
        prob = self._predict_home_prob_model(conn, game_dict)
        probs.append(prob)
    
    print(f"Probability range: {min(probs):.3f} - {max(probs):.3f}")
    print(f"Mean probability: {np.mean(probs):.3f}")
    extreme_probs = sum(1 for p in probs if p < 0.3 or p > 0.7)
    print(f"Extreme predictions (< 30% or > 70%): {extreme_probs}/{len(probs)}")
    
    # 2. Check odds data quality
    odds_check = pd.read_sql_query("""
        SELECT 
            COUNT(*) as total_odds,
            COUNT(DISTINCT sportsbook) as num_books,
            AVG(CAST(odds AS REAL)) as avg_odds,
            MIN(timestamp) as oldest,
            MAX(timestamp) as newest
        FROM odds 
        WHERE market = 'h2h'
        AND timestamp >= datetime('now', '-7 days')
    """, conn)
    
    print(f"\n=== ODDS DATA QUALITY ===")
    print(f"Total odds records: {odds_check.iloc[0]['total_odds']}")
    print(f"Number of sportsbooks: {odds_check.iloc[0]['num_books']}")
    print(f"Average odds: {odds_check.iloc[0]['avg_odds']:.2f}")
    print(f"Data age: {odds_check.iloc[0]['oldest']} to {odds_check.iloc[0]['newest']}")
    
    # 3. Recommendations
    if np.mean(probs) < 0.4 or np.mean(probs) > 0.6:
        print("\n⚠️  WARNING: Model is biased toward one side")
    
    if extreme_probs > len(probs) * 0.3:
        print("\n⚠️  WARNING: Too many extreme predictions - model needs calibration")
    
    if odds_check.iloc[0]['num_books'] < 3:
        print("\n⚠️  WARNING: Limited sportsbook data - edges may be unreliable")

# Fix the edge calculation with proper market efficiency assumptions
def _calculate_realistic_edge(self, model_prob: float, best_odds: int) -> float:
    """
    Calculate edge with market efficiency assumptions
    """
    # Convert American odds to decimal
    if best_odds > 0:
        decimal_odds = 1 + (best_odds / 100.0)
    else:
        decimal_odds = 1 + (100.0 / abs(best_odds))
    
    # Calculate implied probability with juice removed
    raw_implied = 1.0 / decimal_odds
    
    # Assume 4-5% juice/margin (typical for NFL)
    juice_factor = 1.045  # 4.5% house edge
    true_implied = raw_implied * juice_factor
    
    # Conservative edge calculation
    edge = model_prob - true_implied
    
    # Apply market efficiency discount
    # Assume market is 90-95% efficient for NFL
    efficiency_discount = 0.93
    adjusted_edge = edge * efficiency_discount
    
    return adjusted_edge

# Updated value betting scanner with stricter criteria
def _scan_value_bets_conservative(self, days: int = 14, min_edge: float = 0.04):
    """
    More conservative value bet scanning
    """
    today = dt.date.today()
    conn = self._conn()
    
    try:
        # Get upcoming games (shorter window)
        games = pd.read_sql_query("""
            SELECT game_id, away_team AS away, home_team AS home,
                DATE(game_date) AS game_date,
                TIME(start_time_local) AS game_time
            FROM games
            WHERE DATE(game_date) BETWEEN DATE(?) AND DATE(?)
            AND game_date >= DATE('now')  -- Only future games
            ORDER BY game_date, start_time_local
        """, conn, params=[today, today + dt.timedelta(days=days)])
        
        opportunities = []
        
        for _, game in games.iterrows():
            game_dict = {
                'game_id': game['game_id'],
                'home': game['home'],
                'away': game['away'],
                'date': game['game_date']
            }
            
            # Get model probabilities
            ph = self._predict_home_prob_model(conn, game_dict)
            pa = 1.0 - ph
            
            # Apply calibration to reduce overconfidence
            ph_cal = 0.5 + (ph - 0.5) * 0.7  # Shrink toward 50%
            pa_cal = 1.0 - ph_cal
            
            # Get best lines
            lines = self._best_lines_for_game(conn, game['game_id'])
            
            # Check each team for value with stricter criteria
            for team, prob in [(game['home'], ph_cal), (game['away'], pa_cal)]:
                if team in lines:
                    ml_odds = lines[team]['odds']
                    sportsbook = lines[team]['sportsbook']
                    
                    # Use realistic edge calculation
                    edge = self._calculate_realistic_edge(prob, ml_odds)
                    
                    # Stricter filtering criteria
                    if (edge >= min_edge and 
                        prob >= 0.35 and prob <= 0.75 and  # Avoid extreme picks
                        abs(ml_odds) >= 110):  # Avoid picks too close to even
                        
                        # Conservative Kelly sizing
                        decimal_odds = 1 + (ml_odds / 100.0) if ml_odds > 0 else 1 + (100.0 / abs(ml_odds))
                        kelly = ((prob * (decimal_odds - 1)) - (1 - prob)) / (decimal_odds - 1)
                        stake = max(1.0, min(25.0, max(0.0, kelly) * 500.0 * 0.15))  # Fractional Kelly
                        
                        opportunities.append({
                            'game_id': game['game_id'],
                            'game': f"{game['away']} @ {game['home']}",
                            'date': game['game_date'],
                            'team': team,
                            'odds': int(ml_odds),
                            'sportsbook': sportsbook,
                            'model_prob': round(float(prob), 3),
                            'edge_pct': round(float(edge) * 100.0, 1),
                            'recommended_amount': round(stake, 2),
                            'confidence_level': 'low' if edge < 0.06 else 'medium'
                        })
        
        # Sort by edge and limit results
        opportunities.sort(key=lambda x: x['edge_pct'], reverse=True)
        return opportunities[:5]  # Max 5 opportunities
        
    except Exception as e:
        print(f"Conservative value scan error: {e}")
        return []
    finally:
        conn.close()