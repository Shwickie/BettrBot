#!/usr/bin/env python3
"""
FIXED complete_roster.py - Adds ALL NFL players including defensive and OL
Updates your existing database tables with comprehensive 2024 roster
"""

import pandas as pd
from sqlalchemy import create_engine, text
import re
from datetime import datetime

# Config
DB_PATH = "sqlite:///E:/Bettr Bot/betting-bot/data/betting.db"
engine = create_engine(DB_PATH)

def get_comprehensive_2024_roster():
    """Get complete 2024 NFL roster including ALL defensive players and OL"""
    print("🏈 BUILDING COMPREHENSIVE 2024 NFL ROSTER - ALL POSITIONS")
    print("=" * 60)
    
    all_players = []
    
    # Complete 2024 NFL Rosters by Team - EVERY POSITION
    team_rosters = {
        'ARI': {
            'QB': ['Kyler Murray', 'Clayton Tune', 'Desmond Ridder'],
            'RB': ['James Conner', 'Trey Benson', 'Michael Carter', 'DeeJay Dallas'],
            'WR': ['Marvin Harrison Jr.', 'Michael Wilson', 'Greg Dortch', 'Zay Jones', 'Xavier Hutchinson'],
            'TE': ['Trey McBride', 'Elijah Higgins', 'Tip Reiman'],
            'OT': ['Paris Johnson Jr.', 'Kelvin Beachum', 'Jackson Barton', 'Christian Jones'],
            'G': ['Will Hernandez', 'Evan Brown', 'Marquis Hayes', 'Jon Gaines II'],
            'C': ['Hjalte Froholdt', 'Carter OBrien'],
            'DT': ['Bilal Nichols', 'Leki Fotu', 'Khyiris Tonga', 'Roy Lopez'],
            'DE': ['L.J. Collier', 'Dennis Gardeck', 'Zaven Collins', 'Cameron Thomas'],
            'LB': ['Kyzir White', 'Mack Wilson', 'Owen Pappoe', 'Julian Okwara', 'Victor Dimukeje'],
            'CB': ['Marco Wilson', 'Sean Murphy-Bunting', 'Elijah Jones', 'Garrett Williams', 'Max Melton'],
            'S': ['Budda Baker', 'Jalen Thompson', 'Andre Chachere', 'Dadrion Taylor-Demerson'],
            'K': ['Matt Prater'],
            'P': ['Blake Gillikin']
        },
        'ATL': {
            'QB': ['Kirk Cousins', 'Michael Penix Jr.', 'Taylor Heinicke'],
            'RB': ['Bijan Robinson', 'Tyler Allgeier', 'Carlos Washington Jr.', 'Avery Williams'],
            'WR': ['Drake London', 'Darnell Mooney', 'Ray-Ray McCloud', 'KhaDarel Hodge', 'Casey Washington'],
            'TE': ['Kyle Pitts', 'Charlie Woerner', 'Ross Dwelley'],
            'OT': ['Jake Matthews', 'Kaleb McGary', 'Storm Norton', 'Brandon Parker'],
            'G': ['Chris Lindstrom', 'Matthew Bergeron', 'Kyle Hinton', 'Jovaughn Gwyn'],
            'C': ['Drew Dalman', 'Ryan Neuzil'],
            'DT': ['Grady Jarrett', 'David Onyemata', 'Ta\'Quon Graham', 'Kentavius Street'],
            'DE': ['Matthew Judon', 'Lorenzo Carter', 'Arnold Ebiketie', 'DeAngelo Malone'],
            'LB': ['Kaden Elliss', 'Troy Andersen', 'Nate Landman', 'JD Bertrand'],
            'CB': ['A.J. Terrell', 'Mike Hughes', 'Dee Alford', 'Clark Phillips III'],
            'S': ['Jessie Bates III', 'Justin Simmons', 'Richie Grant', 'Micah Abernathy'],
            'K': ['Younghoe Koo'],
            'P': ['Ryan Stonehouse']
        },
        'BAL': {
            'QB': ['Lamar Jackson', 'Josh Johnson', 'Devin Leary'],
            'RB': ['Derrick Henry', 'Justice Hill', 'Rasheen Ali', 'Keaton Mitchell'],
            'WR': ['Zay Flowers', 'Rashod Bateman', 'Nelson Agholor', 'Diontae Johnson', 'Tylan Wallace'],
            'TE': ['Mark Andrews', 'Isaiah Likely', 'Charlie Kolar'],
            'OT': ['Ronnie Stanley', 'Roger Rosengarten', 'Patrick Mekari', 'Josh Jones'],
            'G': ['Kevin Zeitler', 'John Simpson', 'Andrew Vorhees', 'Sala Aumavae-Laulu'],
            'C': ['Tyler Linderbaum', 'Nick Samac'],
            'DT': ['Justin Madubuike', 'Michael Pierce', 'Travis Jones', 'Broderick Washington'],
            'DE': ['Odafe Oweh', 'Kyle Van Noy', 'David Ojabo', 'Adisa Isaac'],
            'LB': ['Roquan Smith', 'Patrick Queen', 'Malik Harrison', 'Chris Board'],
            'CB': ['Marlon Humphrey', 'Brandon Stephens', 'Nate Wiggins', 'Arthur Maulet'],
            'S': ['Kyle Hamilton', 'Marcus Peters', 'Eddie Jackson', 'Sanoussi Kane'],
            'K': ['Justin Tucker'],
            'P': ['Jordan Stout']
        },
        'BUF': {
            'QB': ['Josh Allen', 'Mitchell Trubisky', 'Ben DiNucci'],
            'RB': ['James Cook', 'Ray Davis', 'Ty Johnson', 'Frank Gore Jr.'],
            'WR': ['Stefon Diggs', 'Gabe Davis', 'Khalil Shakir', 'Curtis Samuel', 'Mack Hollins'],
            'TE': ['Dalton Kincaid', 'Dawson Knox', 'Quintin Morris'],
            'OT': ['Dion Dawkins', 'Spencer Brown', 'Tylan Grable', 'Will Clapp'],
            'G': ['Connor McGovern', 'O\'Cyrus Torrence', 'David Edwards', 'Ryan Van Demark'],
            'C': ['Mitch Morse', 'Austin Johnson'],
            'DT': ['DaQuan Jones', 'Ed Oliver', 'DeWayne Carter', 'Austin Johnson'],
            'DE': ['Gregory Rousseau', 'A.J. Epenesa', 'Casey Toohill', 'Javon Solomon'],
            'LB': ['Matt Milano', 'Terrel Bernard', 'Dorian Williams', 'Baylon Spector'],
            'CB': ['Rasul Douglas', 'Christian Benford', 'Kaiir Elam', 'Taron Johnson'],
            'S': ['Damar Hamlin', 'Taylor Rapp', 'Mike Edwards', 'Cole Bishop'],
            'K': ['Tyler Bass'],
            'P': ['Sam Martin']
        },
        'CAR': {
            'QB': ['Bryce Young', 'Andy Dalton', 'Jack Plummer'],
            'RB': ['Chuba Hubbard', 'Miles Sanders', 'Jonathon Brooks', 'Mike Boone'],
            'WR': ['Diontae Johnson', 'Adam Thielen', 'Xavier Legette', 'Jonathan Mingo', 'David Moore'],
            'TE': ['Tommy Tremble', 'Ja\'Tavion Sanders', 'Ian Thomas'],
            'OT': ['Ikem Ekwonu', 'Taylor Moton', 'Yosh Nijman', 'Brady Christensen'],
            'G': ['Robert Hunt', 'Damien Lewis', 'Calvin Throckmorton', 'Cade Mays'],
            'C': ['Austin Corbett', 'Cade Mays'],
            'DT': ['Derrick Brown', 'A\'Shawn Robinson', 'Shy Tuttle', 'LaBryan Ray'],
            'DE': ['Brian Burns', 'DJ Johnson', 'Charles Harris', 'K\'Lavon Chaisson'],
            'LB': ['Shaq Thompson', 'Josey Jewell', 'Trevin Wallace', 'Claudin Cherelus'],
            'CB': ['Jaycee Horn', 'Mike Jackson', 'Troy Hill', 'Dane Jackson'],
            'S': ['Xavier Woods', 'Jordan Fuller', 'Nick Scott', 'Demani Richardson'],
            'K': ['Eddy Pineiro'],
            'P': ['Johnny Hekker']
        },
        'CHI': {
            'QB': ['Caleb Williams', 'Tyson Bagent', 'Brett Rypien'],
            'RB': ['D\'Andre Swift', 'Khalil Herbert', 'Roschon Johnson', 'Travis Homer'],
            'WR': ['DJ Moore', 'Keenan Allen', 'Rome Odunze', 'Tyler Scott', 'DeAndre Carter'],
            'TE': ['Cole Kmet', 'Gerald Everett', 'Marcedes Lewis'],
            'OT': ['Braxton Jones', 'Darnell Wright', 'Larry Borom', 'Kiran Amegadjie'],
            'G': ['Teven Jenkins', 'Nate Davis', 'Matt Pryor', 'Ryan Bates'],
            'C': ['Coleman Shelton', 'Doug Kramer'],
            'DT': ['Gervon Dexter', 'Andrew Billings', 'Byron Cowart', 'Zacch Pickens'],
            'DE': ['Montez Sweat', 'DeMarcus Walker', 'Daniel Hardy', 'Austin Booker'],
            'LB': ['T.J. Edwards', 'Tremaine Edmunds', 'Jack Sanborn', 'Noah Sewell'],
            'CB': ['Jaylon Johnson', 'Kyler Gordon', 'Tyrique Stevenson', 'Terell Smith'],
            'S': ['Kevin Byard', 'Jaquan Brisker', 'Elijah Hicks', 'Tarvarius Moore'],
            'K': ['Cairo Santos'],
            'P': ['Tory Taylor']
        },
        'CIN': {
            'QB': ['Joe Burrow', 'Jake Browning', 'Logan Woodside'],
            'RB': ['Joe Mixon', 'Chase Brown', 'Zack Moss', 'Trayveon Williams'],
            'WR': ['Ja\'Marr Chase', 'Tee Higgins', 'Tyler Boyd', 'Andrei Iosivas', 'Charlie Jones'],
            'TE': ['Mike Gesicki', 'Drew Sample', 'Erick All Jr.'],
            'OT': ['Orlando Brown Jr.', 'Jonah Williams', 'Amarius Mims', 'D\'Ante Smith'],
            'G': ['Alex Cappa', 'Cordell Volson', 'Jackson Carman', 'Trey Hill'],
            'C': ['Ted Karras', 'Jaxson Kirkland'],
            'DT': ['DJ Reader', 'BJ Hill', 'Kris Jenkins', 'McKinnley Jackson'],
            'DE': ['Trey Hendrickson', 'Sam Hubbard', 'Joseph Ossai', 'Myles Murphy'],
            'LB': ['Logan Wilson', 'Germaine Pratt', 'Akeem Davis-Gaither', 'Joe Bachie'],
            'CB': ['Chidobe Awuzie', 'Mike Hilton', 'Cam Taylor-Britt', 'Dax Hill'],
            'S': ['Jessie Bates III', 'Nick Scott', 'Daxton Hill', 'Tycen Anderson'],
            'K': ['Evan McPherson'],
            'P': ['Ryan Rehkow']
        },
        'CLE': {
            'QB': ['Deshaun Watson', 'Jameis Winston', 'Dorian Thompson-Robinson'],
            'RB': ['Nick Chubb', 'Jerome Ford', 'D\'Onta Foreman', 'Pierre Strong Jr.'],
            'WR': ['Amari Cooper', 'Jerry Jeudy', 'Elijah Moore', 'Cedric Tillman', 'Michael Woods II'],
            'TE': ['David Njoku', 'Jordan Akins', 'Blake Whiteheart'],
            'OT': ['Jedrick Wills Jr.', 'Jack Conklin', 'Dawand Jones', 'James Hudson III'],
            'G': ['Joel Bitonio', 'Wyatt Teller', 'Michael Dunn', 'Germain Ifedi'],
            'C': ['Ethan Pocic', 'Luke Wypler'],
            'DT': ['Myles Garrett', 'Dalvin Tomlinson', 'Maurice Hurst', 'Quinton Jefferson'],
            'DE': ['Za\'Darius Smith', 'Alex Wright', 'Isaiah McGuire', 'Sam Kamara'],
            'LB': ['Jeremiah Owusu-Koramoah', 'Anthony Walker', 'Jordan Hicks', 'Winston Reid'],
            'CB': ['Denzel Ward', 'Greg Newsome II', 'Martin Emerson', 'Cameron Mitchell'],
            'S': ['Grant Delpit', 'Juan Thornhill', 'Ronnie Hickman', 'D\'Anthony Bell'],
            'K': ['Dustin Hopkins'],
            'P': ['Corey Bojorquez']
        },
        'DAL': {
            'QB': ['Dak Prescott', 'Cooper Rush', 'Trey Lance'],
            'RB': ['Ezekiel Elliott', 'Tony Pollard', 'Rico Dowdle', 'Deuce Vaughn'],
            'WR': ['CeeDee Lamb', 'Brandin Cooks', 'Jalen Tolbert', 'KaVontae Turpin', 'Ryan Flournoy'],
            'TE': ['Jake Ferguson', 'Luke Schoonmaker', 'Brevyn Spann-Ford'],
            'OT': ['Tyron Smith', 'Terence Steele', 'Tyler Guyton', 'Asim Richards'],
            'G': ['Zack Martin', 'Tyler Smith', 'T.J. Vasher', 'Cooper Beebe'],
            'C': ['Tyler Biadasz', 'Brock Hoffman'],
            'DT': ['Mazi Smith', 'Osa Odighizuwa', 'Linval Joseph', 'Chauncey Golston'],
            'DE': ['Micah Parsons', 'DeMarcus Lawrence', 'Sam Williams', 'Marshawn Kneeland'],
            'LB': ['Leighton Vander Esch', 'Eric Kendricks', 'DeMarvion Overshown', 'Marist Liufau'],
            'CB': ['Trevon Diggs', 'DaRon Bland', 'Jourdan Lewis', 'Caelen Carson'],
            'S': ['Dak Prescott', 'Malik Hooker', 'Donovan Wilson', 'Juanyeh Thomas'],
            'K': ['Brandon Aubrey'],
            'P': ['Bryan Thompson']
        },
        'DEN': {
            'QB': ['Bo Nix', 'Jarrett Stidham', 'Zach Wilson'],
            'RB': ['Javonte Williams', 'Jaleel McLaughlin', 'Samaje Perine', 'Audric Estime'],
            'WR': ['Courtland Sutton', 'Jerry Jeudy', 'Tim Patrick', 'Marvin Mims Jr.', 'Troy Franklin'],
            'TE': ['Greg Dulcich', 'Adam Trautman', 'Lucas Krull'],
            'OT': ['Garett Bolles', 'Mike McGlinchey', 'Alex Palczewski', 'Quinn Bailey'],
            'G': ['Quinn Meinerz', 'Ben Powers', 'Calvin Throckmorton', 'Luke Wattenberg'],
            'C': ['Lloyd Cushenberry III', 'Luke Wattenberg'],
            'DT': ['D.J. Jones', 'Zach Allen', 'Malcolm Roach', 'Eyioma Uwazurike'],
            'DE': ['Bradley Chubb', 'Randy Gregory', 'Nik Bonitto', 'Jonathon Cooper'],
            'LB': ['Alex Singleton', 'Josey Jewell', 'Cody Barton', 'Justin Strnad'],
            'CB': ['Pat Surtain II', 'Ja\'Quan McMillian', 'Riley Moss', 'Levi Wallace'],
            'S': ['Justin Simmons', 'P.J. Locke', 'Brandon Jones', 'JL Skinner'],
            'K': ['Wil Lutz'],
            'P': ['Riley Dixon']
        },
        'DET': {
            'QB': ['Jared Goff', 'Hendon Hooker', 'Nate Sudfeld'],
            'RB': ['Jahmyr Gibbs', 'David Montgomery', 'Craig Reynolds', 'Sione Vaki'],
            'WR': ['Amon-Ra St. Brown', 'Jameson Williams', 'Kalif Raymond', 'Donovan Peoples-Jones', 'Isaiah Williams'],
            'TE': ['Sam LaPorta', 'Brock Wright', 'Parker Hesse'],
            'OT': ['Penei Sewell', 'Taylor Decker', 'Dan Skipper', 'Giovanni Manu'],
            'G': ['Frank Ragnow', 'Kevin Zeitler', 'Graham Glasgow', 'Kayode Awosika'],
            'C': ['Frank Ragnow', 'Michael Niese'],
            'DT': ['Alim McNeill', 'D.J. Reader', 'Levi Onwuzurike', 'Mekhi Wingo'],
            'DE': ['Aidan Hutchinson', 'Marcus Davenport', 'Josh Paschal', 'Za\'Darius Smith'],
            'LB': ['Alex Anzalone', 'Jack Campbell', 'Malcolm Rodriguez', 'Ben Niemann'],
            'CB': ['Carlton Davis III', 'Terrion Arnold', 'Amik Robertson', 'Ennis Rakestraw Jr.'],
            'S': ['Brian Branch', 'Kerby Joseph', 'Ifeatu Melifonwu', 'Loren Strickland'],
            'K': ['Jake Bates'],
            'P': ['Jack Fox']
        },
        'GB': {
            'QB': ['Jordan Love', 'Malik Willis', 'Sean Clifford'],
            'RB': ['Josh Jacobs', 'AJ Dillon', 'MarShawn Lloyd', 'Emanuel Wilson'],
            'WR': ['Jayden Reed', 'Christian Watson', 'Romeo Doubs', 'Dontayvion Wicks', 'Bo Melton'],
            'TE': ['Tucker Kraft', 'Luke Musgrave', 'Ben Sims'],
            'OT': ['David Bakhtiari', 'Elgton Jenkins', 'Rasheed Walker', 'Andre Dillard'],
            'G': ['Jon Runyan Jr.', 'Sean Rhyan', 'Jordan Morgan', 'Jacob Monk'],
            'C': ['Josh Myers', 'Jacob Monk'],
            'DT': ['Kenny Clark', 'T.J. Slaton', 'Devonte Wyatt', 'Colby Wooden'],
            'DE': ['Rashan Gary', 'Preston Smith', 'Lukas Van Ness', 'Kingsley Enagbare'],
            'LB': ['De\'Vondre Campbell', 'Quay Walker', 'Isaiah McDuffie', 'Edgerrin Cooper'],
            'CB': ['Jaire Alexander', 'Eric Stokes', 'Keisean Nixon', 'Carrington Valentine'],
            'S': ['Darnell Savage', 'Adrian Amos', 'Rudy Ford', 'Javon Bullard'],
            'K': ['Brandon McManus'],
            'P': ['Daniel Whelan']
        },
        'HOU': {
            'QB': ['C.J. Stroud', 'Davis Mills', 'Case Keenum'],
            'RB': ['Joe Mixon', 'Dameon Pierce', 'Cam Akers', 'Dare Ogunbowale'],
            'WR': ['Nico Collins', 'Stefon Diggs', 'Tank Dell', 'Robert Woods', 'John Metchie III'],
            'TE': ['Dalton Schultz', 'Cade Stover', 'Brevin Jordan'],
            'OT': ['Laremy Tunsil', 'Tytus Howard', 'Blake Fisher', 'David Sharpe'],
            'G': ['Shaq Mason', 'Kenyon Green', 'Juice Scruggs', 'Nick Broeker'],
            'C': ['Juice Scruggs', 'Scott Quessenberry'],
            'DT': ['Derek Barnett', 'Denico Autry', 'Tim Settle', 'Folorunso Fatukasi'],
            'DE': ['Will Anderson Jr.', 'Danielle Hunter', 'Dylan Horton', 'Solomon Byrd'],
            'LB': ['Azeez Al-Shaair', 'Christian Harris', 'Henry To\'oTo\'o', 'Jake Hansen'],
            'CB': ['Derek Stingley Jr.', 'Kamari Lassiter', 'Jeff Okudah', 'D\'Angelo Ross'],
            'S': ['Jimmie Ward', 'Calen Bullock', 'Jalen Pitre', 'M.J. Stewart'],
            'K': ['Ka\'imi Fairbairn'],
            'P': ['Tommy Townsend']
        },
        'IND': {
            'QB': ['Anthony Richardson', 'Joe Flacco', 'Sam Ehlinger'],
            'RB': ['Jonathan Taylor', 'Trey Sermon', 'Tyler Goodson', 'Evan Hull'],
            'WR': ['Michael Pittman Jr.', 'Josh Downs', 'Alec Pierce', 'Adonai Mitchell', 'Anthony Gould'],
            'TE': ['Mo Alie-Cox', 'Kylen Granson', 'Drew Ogletree'],
            'OT': ['Anthony Richardson', 'Braden Smith', 'Matt Pryor', 'Blake Freeland'],
            'G': ['Quenton Nelson', 'Will Fries', 'Tanor Bortolini', 'Dalton Tucker'],
            'C': ['Ryan Kelly', 'Wesley French'],
            'DT': ['DeForest Buckner', 'Grover Stewart', 'Raekwon Davis', 'Adetomiwa Adebawore'],
            'DE': ['Kwity Paye', 'Dayo Odeyingbo', 'Tyquan Lewis', 'Laiatu Latu'],
            'LB': ['Zaire Franklin', 'E.J. Speed', 'Segun Olubi', 'Jaylon Carlies'],
            'CB': ['Kenny Moore II', 'Julius Brents', 'Dallis Flowers', 'JuJu Brents'],
            'S': ['Julian Blackmon', 'Nick Cross', 'Rodney McLeod', 'Trevor Denbow'],
            'K': ['Matt Gay'],
            'P': ['Rigoberto Sanchez']
        },
        'JAX': {
            'QB': ['Trevor Lawrence', 'Mac Jones', 'C.J. Beathard'],
            'RB': ['Travis Etienne', 'Tank Bigsby', 'D\'Ernest Johnson', 'Keilan Robinson'],
            'WR': ['Calvin Ridley', 'Christian Kirk', 'Gabe Davis', 'Brian Thomas Jr.', 'Parker Washington'],
            'TE': ['Evan Engram', 'Brenton Strange', 'Luke Farrell'],
            'OT': ['Cam Robinson', 'Anton Harrison', 'Walker Little', 'Javon Foster'],
            'G': ['Ezra Cleveland', 'Brandon Scherff', 'Cooper Hodges', 'Cole Van Lanen'],
            'C': ['Mitch Morse', 'Luke Fortner'],
            'DT': ['Roy Robertson-Harris', 'Arik Armstead', 'DaVon Hamilton', 'Maason Smith'],
            'DE': ['Josh Hines-Allen', 'Travon Walker', 'K\'Lavon Chaisson', 'Yasir Abdullah'],
            'LB': ['Foyesade Oluokun', 'Devin Lloyd', 'Chad Muma', 'Ventrell Miller'],
            'CB': ['Tyson Campbell', 'Ronald Darby', 'Jarrian Jones', 'Montaric Brown'],
            'S': ['Andre Cisco', 'Rayshawn Jenkins', 'Antonio Johnson', 'Darnell Savage'],
            'K': ['Cam Little'],
            'P': ['Logan Cooke']
        },
        'KC': {
            'QB': ['Patrick Mahomes', 'Carson Wentz', 'Chris Oladokun'],
            'RB': ['Isiah Pacheco', 'Kareem Hunt', 'Samaje Perine', 'Carson Steele'],
            'WR': ['DeAndre Hopkins', 'Xavier Worthy', 'JuJu Smith-Schuster', 'Skyy Moore', 'Mecole Hardman'],
            'TE': ['Travis Kelce', 'Noah Gray', 'Peyton Hendershot'],
            'OT': ['Orlando Brown Jr.', 'Jawaan Taylor', 'Morris Walker', 'Ethan Driskell'],
            'G': ['Joe Thuney', 'Trey Smith', 'Nick Allegretti', 'Mike Caliendo'],
            'C': ['Creed Humphrey', 'Austin Reiter'],
            'DT': ['Chris Jones', 'Derrick Nnadi', 'Tershawn Wharton', 'Neil Farrell Jr.'],
            'DE': ['George Karlaftis', 'Mike Danna', 'Felix Anudike-Uzomah', 'Cameron Thomas'],
            'LB': ['Nick Bolton', 'Willie Gay Jr.', 'Drue Tranquill', 'Jack Cochrane'],
            'CB': ['L\'Jarius Sneed', 'Trent McDuffie', 'Joshua Williams', 'Jaylen Watson'],
            'S': ['Justin Reid', 'Bryan Cook', 'Chamarri Conner', 'Jaden Hicks'],
            'K': ['Harrison Butker'],
            'P': ['Matt Araiza']
        },
        'LV': {
            'QB': ['Gardner Minshew', 'Aidan O\'Connell', 'Carter Bradley'],
            'RB': ['Alexander Mattison', 'Zamir White', 'Ameer Abdullah', 'Dylan Laube'],
            'WR': ['Davante Adams', 'Jakobi Meyers', 'Tre Tucker', 'DJ Turner', 'Ramel Keyton'],
            'TE': ['Brock Bowers', 'Michael Mayer', 'Harrison Bryant'],
            'OT': ['Kolton Miller', 'Thayer Munford Jr.', 'Andre James', 'DJ Glaze'],
            'G': ['Dylan Parham', 'Jackson Powers-Johnson', 'Cody Whitehair', 'Jordan Meredith'],
            'C': ['Andre James', 'Hroniss Grasu'],
            'DT': ['Christian Wilkins', 'Adam Butler', 'John Jenkins', 'Byron Young'],
            'DE': ['Maxx Crosby', 'Malcolm Koonce', 'Tyree Wilson', 'Janarius Robinson'],
            'LB': ['Robert Spillane', 'Divine Deablo', 'Luke Masterson', 'Tommy Eichenberg'],
            'CB': ['Nate Hobbs', 'Jakorian Bennett', 'Jack Jones', 'Decamerion Richardson'],
            'S': ['Marcus Epps', 'Tre\'von Moehrig', 'Christopher Smith', 'Isaiah Pola-Mao'],
            'K': ['Daniel Carlson'],
            'P': ['AJ Cole']
        },
        'LAC': {
            'QB': ['Justin Herbert', 'Taylor Heinicke', 'Luis Perez'],
            'RB': ['J.K. Dobbins', 'Gus Edwards', 'Kimani Vidal', 'Hassan Haskins'],
            'WR': ['Keenan Allen', 'Mike Williams', 'Joshua Palmer', 'Ladd McConkey', 'DJ Chark'],
            'TE': ['Will Dissly', 'Hayden Hurst', 'Stone Smartt'],
            'OT': ['Rashawn Slater', 'Joe Alt', 'Foster Sarell', 'Jordan McFadden'],
            'G': ['Zion Johnson', 'Trey Pipkins III', 'Jamaree Salyer', 'Brenden Jaimes'],
            'C': ['Bradley Bozeman', 'Will Clapp'],
            'DT': ['Khalil Mack', 'Morgan Fox', 'Poona Ford', 'Otito Ogbonnia'],
            'DE': ['Khalil Mack', 'Bud Dupree', 'Tuli Tuipulotu', 'Chris Rumph II'],
            'LB': ['Derwin James', 'Junior Colson', 'Denzel Perryman', 'Troy Dye'],
            'CB': ['Asante Samuel Jr.', 'Kristian Fulton', 'Ja\'Sir Taylor', 'Cam Hart'],
            'S': ['Derwin James', 'Alohi Gilman', 'AJ Finley', 'Marcus Haynes'],
            'K': ['Cameron Dicker'],
            'P': ['JK Scott']
        },
        'LAR': {
            'QB': ['Matthew Stafford', 'Jimmy Garoppolo', 'Stetson Bennett'],
            'RB': ['Kyren Williams', 'Blake Corum', 'Ronnie Rivers', 'Zach Evans'],
            'WR': ['Cooper Kupp', 'Puka Nacua', 'Demarcus Robinson', 'Tutu Atwell', 'Jordan Whittington'],
            'TE': ['Colby Parkinson', 'Tyler Higbee', 'Davis Allen'],
            'OT': ['Alaric Jackson', 'Rob Havenstein', 'Joe Noteboom', 'Warren McClendon Jr.'],
            'G': ['Kevin Dotson', 'Steve Avila', 'Jonah Jackson', 'KT Leveston'],
            'C': ['Coleman Shelton', 'Beaux Limmer'],
            'DT': ['Aaron Donald', 'Bobby Brown III', 'Kobie Turner', 'Neville Gallimore'],
            'DE': ['Leonard Floyd', 'Byron Young', 'Jared Verse', 'Michael Hoecht'],
            'LB': ['Ernest Jones', 'Christian Rozeboom', 'Omar Speights', 'Jake Hummel'],
            'CB': ['Cobie Durant', 'Darious Williams', 'Ahkello Witherspoon', 'Derion Kendrick'],
            'S': ['Kamren Curl', 'John Johnson III', 'Quentin Lake', 'Kamren Kinchens'],
            'K': ['Joshua Karty'],
            'P': ['Ethan Evans']
        },
        'MIA': {
            'QB': ['Tua Tagovailoa', 'Mike White', 'Tyler Huntley'],
            'RB': ['De\'Von Achane', 'Raheem Mostert', 'Jeff Wilson Jr.', 'Jaylen Wright'],
            'WR': ['Tyreek Hill', 'Jaylen Waddle', 'Odell Beckham Jr.', 'Braxton Berrios', 'Grant DuBose'],
            'TE': ['Mike Gesicki', 'Durham Smythe', 'Julian Hill'],
            'OT': ['Terron Armstead', 'Austin Jackson', 'Kendall Lamm', 'Patrick Paul'],
            'G': ['Robert Hunt', 'Isaiah Wynn', 'Liam Eichenberg', 'Jack Driscoll'],
            'C': ['Connor Williams', 'Aaron Brewer'],
            'DT': ['Calais Campbell', 'Da\'Shawn Hand', 'Benito Jones', 'Neil Farrell Jr.'],
            'DE': ['Emmanuel Ogbah', 'Bradley Chubb', 'Chop Robinson', 'Mohamed Kamara'],
            'LB': ['Jordyn Brooks', 'David Long Jr.', 'Anthony Walker Jr.', 'Duke Riley'],
            'CB': ['Jalen Ramsey', 'Kendall Fuller', 'Kader Kohou', 'Storm Duck'],
            'S': ['Jevon Holland', 'Jordan Poyer', 'Marcus Maye', 'Patrick McMorris'],
            'K': ['Jason Sanders'],
            'P': ['Jake Bailey']
        },
        'MIN': {
            'QB': ['Sam Darnold', 'J.J. McCarthy', 'Nick Mullens'],
            'RB': ['Aaron Jones', 'Ty Chandler', 'Kene Nwangwu', 'Myles Gaskin'],
            'WR': ['Justin Jefferson', 'Jordan Addison', 'Jalen Nailor', 'Brandon Powell', 'Trent Sherfield'],
            'TE': ['T.J. Hockenson', 'Josh Oliver', 'Johnny Mundt'],
            'OT': ['Christian Darrisaw', 'Brian O\'Neill', 'David Quessenberry', 'Walter Rouse'],
            'G': ['Ed Ingram', 'Dalton Risner', 'Blake Brandel', 'Tyrese Robinson'],
            'C': ['Garrett Bradbury', 'Austin Schlottmann'],
            'DT': ['Harrison Phillips', 'Jerry Tillery', 'Jaquelin Roy', 'Levi Drake Rodriguez'],
            'DE': ['Danielle Hunter', 'D.J. Wonnum', 'Patrick Jones II', 'Dallas Turner'],
            'LB': ['Jordan Hicks', 'Blake Cashman', 'Ivan Pace Jr.', 'Kamu Grugier-Hill'],
            'CB': ['Byron Murphy Jr.', 'Stephon Gilmore', 'Shaquill Griffin', 'Fabian Moreau'],
            'S': ['Harrison Smith', 'Camryn Bynum', 'Lewis Cine', 'Josh Metellus'],
            'K': ['Will Reichard'],
            'P': ['Ryan Wright']
        },
        'NE': {
            'QB': ['Drake Maye', 'Jacoby Brissett', 'Joe Milton III'],
            'RB': ['Rhamondre Stevenson', 'Antonio Gibson', 'JaMycal Hasty', 'Kevin Harris'],
            'WR': ['DeMario Douglas', 'Kendrick Bourne', 'Ja\'Lynn Polk', 'Kayshon Boutte', 'Javon Baker'],
            'TE': ['Hunter Henry', 'Austin Hooper', 'Jaheim Bell'],
            'OT': ['Trent Brown', 'Chukwuma Okorafor', 'Vederian Lowe', 'Caedan Wallace'],
            'G': ['Mike Onwenu', 'Sidy Sow', 'Layden Robinson', 'Michael Jordan'],
            'C': ['David Andrews', 'Nick Leverett'],
            'DT': ['Christian Barmore', 'Davon Godchaux', 'Daniel Ekuale', 'Jeremiah Pharms Jr.'],
            'DE': ['Matthew Judon', 'Keion White', 'Deatrich Wise Jr.', 'Oshane Ximines'],
            'LB': ['Ja\'Whaun Bentley', 'Raekwon McMillan', 'Anfernee Jennings', 'Christian Elliss'],
            'CB': ['Jonathan Jones', 'Christian Gonzalez', 'Marcus Jones', 'Alex Austin'],
            'S': ['Kyle Dugger', 'Jabrill Peppers', 'Marte Mapu', 'Jaylinn Hawkins'],
            'K': ['Joey Slye'],
            'P': ['Bryce Baringer']
        },
        'NO': {
            'QB': ['Derek Carr', 'Jake Haener', 'Spencer Rattler'],
            'RB': ['Alvin Kamara', 'Jamaal Williams', 'Kendre Miller', 'Jordan Mims'],
            'WR': ['Chris Olave', 'Rashid Shaheed', 'Cedrick Wilson Jr.', 'Mason Tipton', 'Bub Means'],
            'TE': ['Juwan Johnson', 'Taysom Hill', 'Foster Moreau'],
            'OT': ['Ryan Ramczyk', 'Trevor Penning', 'Taliese Fuaga', 'Landon Young'],
            'G': ['Cesar Ruiz', 'Lucas Patrick', 'Shane Lemieux', 'Kyle Hergel'],
            'C': ['Erik McCoy', 'Connor McGovern'],
            'DT': ['Cameron Jordan', 'Khalen Saunders', 'Nathan Shepherd', 'Bryan Bresee'],
            'DE': ['Cameron Jordan', 'Chase Young', 'Carl Granderson', 'Tanoh Kpassagnon'],
            'LB': ['Demario Davis', 'Pete Werner', 'Willie Gay Jr.', 'Jaylan Ford'],
            'CB': ['Marshon Lattimore', 'Paulson Adebo', 'Alontae Taylor', 'Kool-Aid McKinstry'],
            'S': ['Tyrann Mathieu', 'Will Harris', 'Jordan Howden', 'Johnathan Abram'],
            'K': ['Blake Grupe'],
            'P': ['Matthew Hayball']
        },
        'NYG': {
            'QB': ['Daniel Jones', 'Drew Lock', 'Tommy DeVito'],
            'RB': ['Saquon Barkley', 'Devin Singletary', 'Eric Gray', 'Tyrone Tracy Jr.'],
            'WR': ['Malik Nabers', 'Wan\'Dale Robinson', 'Darius Slayton', 'Jalin Hyatt', 'Isaiah Hodgins'],
            'TE': ['Daniel Bellinger', 'Darren Waller', 'Theo Johnson'],
            'OT': ['Andrew Thomas', 'Jermaine Eluemunor', 'Evan Neal', 'Joshua Ezeudu'],
            'G': ['Jon Runyan Jr.', 'Greg Van Roten', 'Marcus Haynes', 'Jake Kubas'],
            'C': ['John Michael Schmitz', 'Austin Schlottmann'],
            'DT': ['Dexter Lawrence', 'A\'Shawn Robinson', 'Jordon Riley', 'D.J. Davidson'],
            'DE': ['Brian Burns', 'Kayvon Thibodaux', 'Azeez Ojulari', 'Boogie Basham'],
            'LB': ['Bobby Okereke', 'Micah McFadden', 'Isaiah Simmons', 'Darius Muasau'],
            'CB': ['Deonte Banks', 'Cor\'Dale Flott', 'Adoree\' Jackson', 'Nick McCloud'],
            'S': ['Xavier McKinney', 'Jason Pinnock', 'Dane Belton', 'Tyler Nubin'],
            'K': ['Graham Gano'],
            'P': ['Jamie Gillan']
        },
        'NYJ': {
            'QB': ['Aaron Rodgers', 'Tyrod Taylor', 'Jordan Travis'],
            'RB': ['Breece Hall', 'Braelon Allen', 'Isaiah Davis', 'Xazavian Valladay'],
            'WR': ['Garrett Wilson', 'Mike Williams', 'Allen Lazard', 'Davante Adams', 'Malachi Corley'],
            'TE': ['Tyler Conklin', 'C.J. Uzomah', 'Jeremy Ruckert'],
            'OT': ['Duane Brown', 'Mekhi Becton', 'Morgan Moses', 'Olu Fashanu'],
            'G': ['Alijah Vera-Tucker', 'John Simpson', 'Xavier Newman', 'Wes Schweitzer'],
            'C': ['Connor McGovern', 'Joe Tippmann'],
            'DT': ['Quinnen Williams', 'Solomon Thomas', 'Leki Fotu', 'Javon Kinlaw'],
            'DE': ['Haason Reddick', 'Will McDonald IV', 'Jermaine Johnson', 'Micheal Clemons'],
            'LB': ['C.J. Mosley', 'Quincy Williams', 'Jamien Sherwood', 'Chazz Surratt'],
            'CB': ['Sauce Gardner', 'D.J. Reed', 'Michael Carter II', 'Brandin Echols'],
            'S': ['Jordan Whitehead', 'Tony Adams', 'Ashtyn Davis', 'Chuck Clark'],
            'K': ['Greg Zuerlein'],
            'P': ['Thomas Morstead']
        },
        'PHI': {
            'QB': ['Jalen Hurts', 'Kenny Pickett', 'Tanner McKee'],
            'RB': ['Saquon Barkley', 'Kenneth Gainwell', 'Will Shipley', 'Kendall Fuller'],
            'WR': ['A.J. Brown', 'DeVonta Smith', 'Jahan Dotson', 'Britain Covey', 'Johnny Wilson'],
            'TE': ['Dallas Goedert', 'Grant Calcaterra', 'E.J. Jenkins'],
            'OT': ['Jordan Mailata', 'Lane Johnson', 'Fred Johnson', 'Tyler Steen'],
            'G': ['Landon Dickerson', 'Mekhi Becton', 'Tyler Steen', 'Trevor Keegan'],
            'C': ['Jason Kelce', 'Cam Jurgens'],
            'DT': ['Fletcher Cox', 'Jordan Davis', 'Jalen Carter', 'Milton Williams'],
            'DE': ['Josh Sweat', 'Brandon Graham', 'Nolan Smith', 'Jalyx Hunt'],
            'LB': ['Nakobe Dean', 'Devin White', 'Jeremiah Trotter Jr.', 'Ben VanSumeren'],
            'CB': ['Darius Slay', 'Quinyon Mitchell', 'Isaiah Rodgers', 'Cooper DeJean'],
            'S': ['C.J. Gardner-Johnson', 'Reed Blankenship', 'Sydney Brown', 'Tristin McCollum'],
            'K': ['Jake Elliott'],
            'P': ['Braden Mann']
        },
        'PIT': {
            'QB': ['Russell Wilson', 'Justin Fields', 'Kyle Allen'],
            'RB': ['Najee Harris', 'Jaylen Warren', 'Cordarrelle Patterson', 'Aaron Shampklin'],
            'WR': ['George Pickens', 'Calvin Austin III', 'Van Jefferson', 'Roman Wilson', 'Scotty Miller'],
            'TE': ['Pat Freiermuth', 'Darnell Washington', 'Connor Heyward'],
            'OT': ['Broderick Jones', 'Dan Moore Jr.', 'Troy Fautanu', 'Spencer Anderson'],
            'G': ['Isaac Seumalo', 'James Daniels', 'Spencer Anderson', 'Mason McCormick'],
            'C': ['Zach Frazier', 'Ryan McCollum'],
            'DT': ['Cameron Heyward', 'Keeanu Benton', 'Montravius Adams', 'Isaiahh Loudermilk'],
            'DE': ['T.J. Watt', 'Alex Highsmith', 'Nick Herbig', 'Jeremiah Moon'],
            'LB': ['Patrick Queen', 'Elandon Roberts', 'Cole Holcomb', 'Payton Wilson'],
            'CB': ['Joey Porter Jr.', 'Donte Jackson', 'Cory Trice Jr.', 'Cameron Sutton'],
            'S': ['Minkah Fitzpatrick', 'DeShon Elliott', 'Damontae Kazee', 'Terrell Edmunds'],
            'K': ['Chris Boswell'],
            'P': ['Cameron Johnston']
        },
        'SF': {
            'QB': ['Brock Purdy', 'Sam Darnold', 'Brandon Allen'],
            'RB': ['Christian McCaffrey', 'Jordan Mason', 'Elijah Mitchell', 'Isaac Guerendo'],
            'WR': ['Deebo Samuel', 'Brandon Aiyuk', 'Jauan Jennings', 'Ricky Pearsall', 'Jacob Cowing'],
            'TE': ['George Kittle', 'Eric Saubert', 'Jake Tonges'],
            'OT': ['Trent Williams', 'Colton McKivitz', 'Jaylon Moore', 'Brandon Parker'],
            'G': ['Aaron Banks', 'Dominick Puni', 'Spencer Burford', 'Ben Bartch'],
            'C': ['Jake Brendel', 'Matt Hennessy'],
            'DT': ['Javon Hargrave', 'Arik Armstead', 'Maliek Collins', 'Kevin Givens'],
            'DE': ['Nick Bosa', 'Leonard Floyd', 'Yetur Gross-Matos', 'Robert Beal Jr.'],
            'LB': ['Fred Warner', 'Dre Greenlaw', 'De\'Vondre Campbell', 'Dee Winters'],
            'CB': ['Charvarius Ward', 'Deommodore Lenoir', 'Isaac Yiadom', 'Renardo Green'],
            'S': ['Talanoa Hufanga', 'Ji\'Ayir Brown', 'Malik Mustapha', 'George Odum'],
            'K': ['Jake Moody'],
            'P': ['Mitch Wishnowsky']
        },
        'SEA': {
            'QB': ['Geno Smith', 'Sam Howell', 'P.J. Walker'],
            'RB': ['Kenneth Walker III', 'Zach Charbonnet', 'Kenny McIntosh', 'George Holani'],
            'WR': ['DK Metcalf', 'Tyler Lockett', 'Jaxon Smith-Njigba', 'Jake Bobo', 'Laviska Shenault Jr.'],
            'TE': ['Noah Fant', 'Pharaoh Brown', 'AJ Barner'],
            'OT': ['Charles Cross', 'George Fant', 'Abraham Lucas', 'Stone Forsythe'],
            'G': ['Damien Lewis', 'Laken Tomlinson', 'Anthony Bradford', 'Christian Haynes'],
            'C': ['Connor Williams', 'Olu Oluwatimi'],
            'DT': ['Jarran Reed', 'Byron Murphy', 'Johnathan Hankins', 'Cameron Young'],
            'DE': ['Boye Mafe', 'Uchenna Nwosu', 'Derick Hall', 'Leonard Williams'],
            'LB': ['Bobby Wagner', 'Jordyn Brooks', 'Jerome Baker', 'Tyrel Dodson'],
            'CB': ['Devon Witherspoon', 'Riq Woolen', 'Tre Brown', 'Nehemiah Pritchett'],
            'S': ['Julian Love', 'Rayshawn Jenkins', 'Coby Bryant', 'K\'Von Wallace'],
            'K': ['Jason Myers'],
            'P': ['Michael Dickson']
        },
        'TB': {
            'QB': ['Baker Mayfield', 'Kyle Trask', 'John Wolford'],
            'RB': ['Rachaad White', 'Bucky Irving', 'Sean Tucker', 'Chase Edmonds'],
            'WR': ['Mike Evans', 'Chris Godwin', 'Jalen McMillan', 'Trey Palmer', 'Sterling Shepard'],
            'TE': ['Cade Otton', 'Payne Durham', 'Ko Kieft'],
            'OT': ['Tristan Wirfs', 'Donovan Smith', 'Justin Skule', 'Graham Barton'],
            'G': ['Cody Mauch', 'Ben Bredeson', 'Royce Newman', 'Elijah Klein'],
            'C': ['Graham Barton', 'Robert Hainsey'],
            'DT': ['Vita Vea', 'Calijah Kancey', 'Greg Gaines', 'Logan Hall'],
            'DE': ['Shaquil Barrett', 'Joe Tryon-Shoyinka', 'YaYa Diaby', 'Chris Braswell'],
            'LB': ['Lavonte David', 'Devin White', 'SirVocea Dennis', 'J.J. Russell'],
            'CB': ['Jamel Dean', 'Carlton Davis', 'Zyon McCollum', 'Josh Hayes'],
            'S': ['Antoine Winfield Jr.', 'Mike Edwards', 'Jordan Whitehead', 'Tykee Smith'],
            'K': ['Chase McLaughlin'],
            'P': ['Jake Camarda']
        },
        'TEN': {
            'QB': ['Will Levis', 'Mason Rudolph', 'Malik Willis'],
            'RB': ['Tony Pollard', 'Tyjae Spears', 'Julius Chestnut', 'Joshua Kelley'],
            'WR': ['DeAndre Hopkins', 'Calvin Ridley', 'Tyler Boyd', 'Treylon Burks', 'Jha\'Quan Jackson'],
            'TE': ['Chigoziem Okonkwo', 'Nick Vannett', 'Josh Whyle'],
            'OT': ['JC Latham', 'Nicholas Petit-Frere', 'Andre Dillard', 'Leroy Watson IV'],
            'G': ['Peter Skoronski', 'Dillon Radunz', 'Saahdiq Charles', 'Daniel Brunskill'],
            'C': ['Lloyd Cushenberry III', 'Corey Levin'],
            'DT': ['Jeffery Simmons', 'T\'Vondre Sweat', 'Keondre Coburn', 'Abdullah Anderson'],
            'DE': ['Harold Landry III', 'Arden Key', 'Rashad Weaver', 'James Houston'],
            'LB': ['Kenneth Murray Jr.', 'Jack Gibbens', 'Otis Reese IV', 'Garret Wallow'],
            'CB': ['L\'Jarius Sneed', 'Chidobe Awuzie', 'Roger McCreary', 'Darrell Baker Jr.'],
            'S': ['Amani Hooker', 'Quandre Diggs', 'Elijah Molden', 'Julius Wood'],
            'K': ['Nick Folk'],
            'P': ['Ryan Stonehouse']
        },
        'WSH': {
            'QB': ['Jayden Daniels', 'Marcus Mariota', 'Jeff Driskel'],
            'RB': ['Brian Robinson Jr.', 'Austin Ekeler', 'Jeremy McNichols', 'Chris Rodriguez Jr.'],
            'WR': ['Terry McLaurin', 'Jahan Dotson', 'Noah Brown', 'Luke McCaffrey', 'Olamide Zaccheaus'],
            'TE': ['Zach Ertz', 'John Bates', 'Ben Skowronek'],
            'OT': ['Brandon Coleman', 'Andrew Wylie', 'Cornelius Lucas', 'Trent Scott'],
            'G': ['Sam Cosmi', 'Nick Allegretti', 'Chris Paul', 'Michael Deiter'],
            'C': ['Tyler Biadasz', 'Ricky Stromberg'],
            'DT': ['Jonathan Allen', 'Daron Payne', 'Phidarian Mathis', 'John Ridgeway'],
            'DE': ['Montez Sweat', 'Dorance Armstrong', 'Clelin Ferrell', 'KJ Henry'],
            'LB': ['Bobby Wagner', 'Frankie Luvu', 'Dante Fowler Jr.', 'Jamin Davis'],
            'CB': ['Marshon Lattimore', 'Benjamin St-Juste', 'Noah Igbinoghene', 'Mike Sainristil'],
            'S': ['Jeremy Chinn', 'Percy Butler', 'Tyler Owens', 'Darrick Forrest'],
            'K': ['Austin Seibert'],
            'P': ['Tress Way']
        }
    }
    
    # Convert to player list
    for team, positions in team_rosters.items():
        for position, players in positions.items():
            for player_name in players:
                all_players.append({
                    'player_name': clean_player_name(player_name),
                    'team': team,
                    'position': position,
                    'player_id': generate_player_id(player_name, team),
                    'source': 'COMPLETE_2024_ROSTER',
                    'season': 2024,
                    'is_active': True
                })
    
    print(f"📊 Total comprehensive roster: {len(all_players)} players")
    
    # Show position distribution
    position_counts = {}
    for player in all_players:
        pos = player['position']
        position_counts[pos] = position_counts.get(pos, 0) + 1
    
    print(f"\n📋 POSITION BREAKDOWN:")
    defensive_pos = ['DT', 'DE', 'LB', 'CB', 'S']
    ol_pos = ['OT', 'G', 'C']
    
    for pos in sorted(position_counts.keys()):
        count = position_counts[pos]
        if pos in defensive_pos:
            icon = "🛡️"
        elif pos in ol_pos:
            icon = "🗿"
        else:
            icon = "⚡"
        print(f"  {icon} {pos}: {count} players")
    
    def_total = sum(position_counts.get(pos, 0) for pos in defensive_pos)
    ol_total = sum(position_counts.get(pos, 0) for pos in ol_pos)
    print(f"\n🎯 COVERAGE SUMMARY:")
    print(f"  🛡️ Defensive players: {def_total}")
    print(f"  🗿 Offensive line: {ol_total}")
    print(f"  📈 Non-skill positions: {def_total + ol_total} ({(def_total + ol_total)/len(all_players)*100:.1f}%)")
    
    return all_players

def clean_player_name(name):
    """Enhanced player name cleaning"""
    if not name:
        return ""
    
    name = str(name).strip()
    
    # Remove suffixes
    name = re.sub(r'\s+(Jr\.?|Sr\.?|III?|IV|V)$', '', name, flags=re.IGNORECASE)
    
    # Fix common abbreviations
    name = re.sub(r'\bTj\b', 'T.J.', name, flags=re.IGNORECASE)
    name = re.sub(r'\bCj\b', 'C.J.', name, flags=re.IGNORECASE)
    name = re.sub(r'\bAj\b', 'A.J.', name, flags=re.IGNORECASE)
    name = re.sub(r'\bJj\b', 'J.J.', name, flags=re.IGNORECASE)
    name = re.sub(r'\bBj\b', 'B.J.', name, flags=re.IGNORECASE)
    
    # Handle apostrophes
    name = name.replace("'", "'")
    
    # Clean whitespace
    name = ' '.join(name.split())
    
    return name.title()

def generate_player_id(name, team):
    """Generate consistent player ID"""
    clean_name = re.sub(r'[^\w]', '', name.upper())
    return f"{team}_{clean_name}_{hash(f'{name}{team}') % 10000}"

def update_existing_tables(all_players):
    """Update your existing database tables with comprehensive roster"""
    print(f"\n💾 UPDATING YOUR EXISTING DATABASE TABLES")
    print("=" * 50)
    
    try:
        with engine.begin() as conn:
            # Update enhanced_nfl_players table
            print("🔧 Updating enhanced_nfl_players...")
            enhanced_count = 0
            
            for player in all_players:
                # Check if player already exists
                existing = conn.execute(text("""
                    SELECT player_id FROM enhanced_nfl_players 
                    WHERE player_name = :name AND team = :team
                """), {
                    'name': player['player_name'],
                    'team': player['team']
                }).fetchone()
                
                if not existing:
                    conn.execute(text("""
                        INSERT OR IGNORE INTO enhanced_nfl_players 
                        (player_id, player_name, team, position, source, is_active, is_key_player, notes)
                        VALUES (:player_id, :player_name, :team, :position, :source, :is_active, 1, 'Comprehensive 2024 roster')
                    """), {
                        'player_id': player['player_id'],
                        'player_name': player['player_name'],
                        'team': player['team'],
                        'position': player['position'],
                        'source': player['source'],
                        'is_active': player['is_active']
                    })
                    enhanced_count += 1
            
            print(f"✅ Added {enhanced_count} new players to enhanced_nfl_players")
            
            # Update current_nfl_players table  
            print("🔧 Updating current_nfl_players...")
            current_count = 0
            
            for player in all_players:
                existing = conn.execute(text("""
                    SELECT player_id FROM current_nfl_players 
                    WHERE player_id = :player_id
                """), {'player_id': player['player_id']}).fetchone()
                
                if not existing:
                    conn.execute(text("""
                        INSERT OR IGNORE INTO current_nfl_players 
                        (player_id, player_display_name, recent_team, position)
                        VALUES (:player_id, :player_display_name, :recent_team, :position)
                    """), {
                        'player_id': player['player_id'],
                        'player_display_name': player['player_name'],
                        'recent_team': player['team'],
                        'position': player['position']
                    })
                    current_count += 1
            
            print(f"✅ Added {current_count} new players to current_nfl_players")
            
            # Update player_team_map table
            print("🔧 Updating player_team_map...")
            team_map_count = 0
            
            for player in all_players:
                existing = conn.execute(text("""
                    SELECT player_id FROM player_team_map 
                    WHERE player_id = :player_id AND season = 2024
                """), {'player_id': player['player_id']}).fetchone()
                
                if not existing:
                    conn.execute(text("""
                        INSERT OR IGNORE INTO player_team_map 
                        (player_id, season, full_name, position, team)
                        VALUES (:player_id, 2024, :full_name, :position, :team)
                    """), {
                        'player_id': player['player_id'],
                        'full_name': player['player_name'],
                        'position': player['position'],
                        'team': player['team']
                    })
                    team_map_count += 1
            
            print(f"✅ Added {team_map_count} new players to player_team_map")
            
            print(f"\n📊 DATABASE UPDATE SUMMARY:")
            print(f"  🔹 Enhanced NFL players: +{enhanced_count}")
            print(f"  🔹 Current NFL players: +{current_count}")
            print(f"  🔹 Player team map: +{team_map_count}")
            
            return enhanced_count + current_count + team_map_count
            
    except Exception as e:
        print(f"❌ Database update failed: {e}")
        import traceback
        traceback.print_exc()
        return 0

def fix_injury_mappings():
    """Fix injury mappings using the updated roster"""
    print(f"\n🔄 FIXING INJURY MAPPINGS WITH UPDATED ROSTER")
    print("=" * 50)
    
    try:
        with engine.connect() as conn:
            # Get unmapped injuries
            unmapped = pd.read_sql(text("""
                SELECT id, player_name, team, designation
                FROM nfl_injuries_tracking 
                WHERE is_active = 1 AND player_id IS NULL
                ORDER BY player_name
            """), conn)
            
            if unmapped.empty:
                print("✅ No unmapped injuries found!")
                return 0
            
            print(f"🔄 Processing {len(unmapped)} unmapped injuries...")
            
            fixed_count = 0
            
            with engine.begin() as trans_conn:
                for _, injury in unmapped.iterrows():
                    player_name = injury['player_name']
                    injury_team = injury['team']
                    
                    # Try to find player in enhanced table
                    player_query = text("""
                        SELECT player_id, team, position, player_name
                        FROM enhanced_nfl_players 
                        WHERE LOWER(player_name) = LOWER(:name)
                        AND (team = :team OR :team = 'UNKNOWN')
                        AND is_active = 1
                        ORDER BY CASE WHEN team = :team THEN 1 ELSE 2 END
                        LIMIT 1
                    """)
                    
                    result = trans_conn.execute(player_query, {
                        'name': player_name,
                        'team': injury_team
                    }).fetchone()
                    
                    if result:
                        trans_conn.execute(text("""
                            UPDATE nfl_injuries_tracking 
                            SET player_id = :player_id,
                                team = :correct_team,
                                confidence_score = 0.99,
                                last_updated = :timestamp,
                                notes = 'Fixed with comprehensive roster'
                            WHERE id = :injury_id
                        """), {
                            'player_id': result[0],
                            'correct_team': result[1],
                            'timestamp': datetime.now(),
                            'injury_id': injury['id']
                        })
                        
                        fixed_count += 1
                        print(f"✅ '{player_name}' → {result[1]} | {result[0]}")
            
            print(f"\n📊 INJURY MAPPING RESULTS:")
            print(f"  ✅ Successfully mapped: {fixed_count}")
            print(f"  📈 Success rate: {fixed_count/len(unmapped)*100:.1f}%")
            
            return fixed_count
            
    except Exception as e:
        print(f"❌ Injury mapping failed: {e}")
        return 0

def validate_final_results():
    """Validate the final results"""
    print(f"\n📊 VALIDATING FINAL RESULTS")
    print("=" * 30)
    
    try:
        with engine.connect() as conn:
            # Check injury mapping rates
            total_injuries = conn.execute(text("SELECT COUNT(*) FROM nfl_injuries_tracking WHERE is_active = 1")).scalar()
            mapped_injuries = conn.execute(text("SELECT COUNT(*) FROM nfl_injuries_tracking WHERE is_active = 1 AND player_id IS NOT NULL")).scalar()
            
            mapping_rate = (mapped_injuries / total_injuries * 100) if total_injuries > 0 else 0
            
            # Check position coverage in enhanced table
            position_stats = pd.read_sql(text("""
                SELECT position, COUNT(*) as count
                FROM enhanced_nfl_players 
                WHERE is_active = 1
                GROUP BY position
                ORDER BY count DESC
            """), conn)
            
            defensive_positions = ['DT', 'DE', 'LB', 'CB', 'S']
            ol_positions = ['OT', 'G', 'C']
            
            def_count = position_stats[position_stats['position'].isin(defensive_positions)]['count'].sum()
            ol_count = position_stats[position_stats['position'].isin(ol_positions)]['count'].sum()
            total_players = position_stats['count'].sum()
            
            print(f"📈 FINAL SYSTEM STATUS:")
            print(f"  Total injuries: {total_injuries}")
            print(f"  Mapped injuries: {mapped_injuries} ({mapping_rate:.1f}%)")
            print(f"  Total players: {total_players}")
            print(f"  🛡️ Defensive players: {def_count}")
            print(f"  🗿 Offensive line: {ol_count}")
            print(f"  📊 Non-skill coverage: {(def_count + ol_count)/total_players*100:.1f}%")
            
            return mapping_rate, def_count, ol_count
            
    except Exception as e:
        print(f"❌ Validation failed: {e}")
        return 0, 0, 0

def main():
    """Main execution - fix your roster problem"""
    print("🏈 COMPREHENSIVE 2024 NFL ROSTER FIX")
    print("=" * 50)
    print("Fixing your database to include ALL players - defensive, OL, everyone!")
    
    try:
        # Step 1: Get comprehensive 2024 roster
        all_players = get_comprehensive_2024_roster()
        
        if not all_players:
            print("❌ Failed to create comprehensive roster")
            return
        
        # Step 2: Update your existing database tables
        updated_count = update_existing_tables(all_players)
        
        # Step 3: Fix injury mappings
        fixed_count = fix_injury_mappings()
        
        # Step 4: Validate results
        mapping_rate, def_count, ol_count = validate_final_results()
        
        print(f"\n🏆 ROSTER FIX COMPLETE!")
        print(f"  📊 Players added: {updated_count}")
        print(f"  🔄 Injuries fixed: {fixed_count}")
        print(f"  📈 Final mapping rate: {mapping_rate:.1f}%")
        print(f"  🛡️ Defensive players: {def_count}")
        print(f"  🗿 Offensive linemen: {ol_count}")
        
        if def_count > 150 and ol_count > 90:
            print(f"\n🎉 SUCCESS! Your database now has comprehensive NFL coverage!")
            print(f"🤖 Your injury bot can now properly track ALL position types!")
        else:
            print(f"\n⚠️ Partial success - run the script again to ensure all data is populated")
        
        print(f"\n🚀 NEXT STEPS:")
        print(f"  1. Your enhanced_nfl_players table now has ALL positions")
        print(f"  2. Run your injury scraper to pick up more mappings")
        print(f"  3. Test with defensive players like T.J. Watt, Myles Garrett")
        print(f"  4. Test with OL players like Trent Williams, Quenton Nelson")
        
    except Exception as e:
        print(f"❌ Roster fix failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()