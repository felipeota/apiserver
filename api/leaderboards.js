var db = require(__dirname + "/database.js"),
    md5 = require(__dirname + "/md5.js"),
    utils = require(__dirname + "/utils.js"),
    datetime = require(__dirname + "/datetime.js"),
    errorcodes = require(__dirname + "/errorcodes.js").errorcodes,
    agora = require("agoragames-leaderboard"),
    url = require('url'),
    redis = require('redis'),
    redisURL = url.parse(process.env.REDISCLOUD_URL || "redis://localhost:6379");
var client = redis.createClient(redisURL.port, redisURL.hostname, {'no_ready_check': true});
if (redisURL.auth && redisURL.auth.split(":").length > 1) {
  client.auth(redisURL.auth.split(":")[1]);
}
var redisOptions = {};
var lb = new agora("leaderboard", null, {'redis_connection':client});
lb.rankMemberIn('test_level', 'pepe', 25, 'pepe', function(reply){
  console.log(reply);
})

var leaderboards = module.exports = {

    /**
     * Lists scores from a leaderboard table
     * @param options:  table, url, highest, mode, page, perpage, filters ass. array, friendslist,
     * @param callback function (error, errorcode, numscores, scores)
     */

    global: function(options, callback){

    },

    list: function(options, callback) {

        // filtering for playerids
        var playerids = options.friendslist || [];

        console.log(playerids);
        console.log(options.playerid);
        if (playerids.length > 0){
          console.log(playerids);
          playerids.push(options.playerid);
          lb.rankedInListIn(options.table, playerids, {'withMemberData':true}, function(ranks){
            console.log(ranks);
            cleanRanks(ranks);
            callback(null, errorcodes.NoError, 0, ranks);
          });
        }
        else{
          console.log("around me");
          lb.aroundMeIn(options.table, options.playerid, {'withMemberData':true}, function(ranks){
            console.log(ranks);
            cleanRanks(ranks);
            callback(null, errorcodes.NoError, 0, ranks);
          });
        }
    },

    /**
     * Saves a score
     * @param options: url, name, points, auth, playerid, table, highest, allowduplicates, customfields ass. array
     * @param callback function(error, errorcode)
     */
    save: function(options, callback) {
        console.log("saving");
        if(!options.playername) {
            callback("no name (" + options.playername + ")", errorcodes.InvalidName);
            return;
        }

        if(!options.table) {
            callback("no table name (" + options.table + ")", errorcodes.InvalidLeaderboardId);
            return;
        }

        options.highest = options.lowest !== true;

        // small cleanup
        var score = {};

	score.date = datetime.now;

        // insert
        lb.rankMemberIn(options.table, options.playerid, options.points, options.playername, function(reply){
          callback(null, errorcodes.NoError, 0, options);
        });
    },

    saveAndList: function(options, callback) {
        leaderboards.save(options, function(error, errorcode, insertedid, insertedscore) {
            if(error) {
                return callback(error + " (api.leaderboards.saveAndList:232)", errorcode);
            }

            var query = {
                table: options.table
            };

            if(options.excludeplayerid !== true && options.playerid) {
                query.playerid = options.playerid;
            }

            if(options.fields && Object.keys(options.fields).length > 0) {
                query.fields = options.fields;
            }

            if(options.hasOwnProperty("global")) {
                query.global = options.global;
            }

            if(options.hasOwnProperty("source")) {
                query.source = options.source;
            }

            // find the offset
            rank(query, options.highest, options.points, function(error, errorcode, before) {
                if(error) {
                    return callback(error + " (api.leaderboards.saveAndList:240)", errorcode);
                }
                // prepare our query for listing
                query.publickey = options.publickey;
                query.perpage = options.perpage;
                query.page = Math.floor(before / options.perpage) + 1;
                query.highest = options.lowest !== true;
                query.lowest = !query.highest;
                delete(query.points);
                leaderboards.list(query, function(error, errorcode, numscores, scores) {

                        if(scores && scores.length) {
                        for(var i=0, len=scores.length; i<len; i++) {
                            if(scores[i].points == options.points &&
                               scores[i].playerid == options.playerid &&
                               scores[i].playername == options.playername &&
                               scores[i].source == options.source) {
                                   scores[i].submitted = true;
                                   break;
                               }
                        }
                    }

                    return callback(error, errorcode, numscores, scores);
                });
            });
        });
    }
};

function cleanRanks(ranks){
  var i, len;
  for(i=0, len=ranks.length; i<len; i++){
    var rank = ranks[i];
    rank.points = rank.score || 0;
    rank.playerid = rank.member;
    rank.playername = rank.member_data || "";
  }
}

/**
 * Strips unnceessary data and tidies a score object
 */
function clean(scores, baserank) {
    var i, len, x, score,
        results = [];

    for(i=0, len=scores.length; i<len; i++) {
        score = scores[i].toObject();

        for(x in score) {
            if(typeof(score[x]) == "string") {
                score[x] = utils.unescape(score[x]);
            }
        }

        for(x in score.fields) {
            if(typeof(score.fields[x]) == "string") {
                score.fields[x] = utils.unescape(score.fields[x]);
            }
        }

        score.rank = baserank + i;
        score.scoreid = score._id.toString();
		score.rdate = utils.friendlyDate(utils.fromTimestamp(score.date));
        delete score._id;
        delete score.__v;
        delete score.hash;
        results.push(score);
    }
    return results;
}


// indexes provide a way to skip expensive mongodb count operations by tracking
// the rank position of points for specific leaderboard queries

var indexlist = [];
var index = {};/*
    hash: {
        hash: hash,
        query: query,
        highest: highest,
        scores: [ { points: 1000, scores: 7 }, { points: 989, scores: 1}],
        remove: [ { points: 1000, 3 } ],
        lastupdated: timestamp,
        lastused: timestamp
}*/


/*
 * Gets the rank of the provided points based on its query either from an existing
 * index, or manually while it creates a new index
 */
function rank(query, highest, points, callback) {

    var hash = md5(JSON.stringify(query) + "." + highest),
        newscore = { points: points, scores: 1, before: 0 },
        ind = index[hash];

    if(ind) {

        ind.lastused = datetime.now;
        if(ind.removeHash[points]) {
            return callback(null, errorcodes.NoError, ind.removeHash[points].before);
        }

        addToIndex(ind, highest, newscore, function(o) {
            return callback(null, newscore.before);
        });
    }

    // set up our new index and chek against the database
    index[hash] = {
        key: hash,
        query: query,
        highest: highest,
        scores: [newscore],
        remove: [newscore],
        removeHash: {},
        lastupdated: 0,
        lastused: datetime.now
    };

    indexlist.push(index[hash]);
    indexlist.sort(function(a, b) { // todo: this could be better
        return a.lastupdated < b.lastupdated ? 1 : -1;
    });

    query.points = highest ? { $gte: points } : { $lte: points };
    db.LeaderboardScore.count(query, function(error, numscores) {
        return callback(error, error ? errorcodes.GeneralError : errorcodes.NoError, numscores);
    });
}

// asynchronously puts a new score in an index
function addToIndex(index, highest, newscore, callback) {
    var ai, i, len,
        found = false;

    function nextBlock() {
        len = i + 1000;
        if(len >= index.scores.length) {
            len = index.scores.length;
        }

        if(i >= len) {
            return callback(newscore);
        }

        for(i=i; i<len; i++) {
            ai = index.scores[i];

            // update the scores after us to have one more 'before'
            if(found) {
                ai.before++;
                continue;
            }

            // count the number of scores higher or lower depending on setting
            found = (highest && ai.points > newscore.points) || (!highest && ai.points < newscore.points);

            if(!found) {
                newscore.before += ai.scores;
                continue;
            }

            // insert our new score
            index.scores.splice(i, 0, newscore);
            index.remove.push(newscore);
            index.removeHash[newscore.points] = newscore;
       }

       return setTimeout(nextBlock, 100);
    }

    nextBlock();
}

// keep our indexes up to date
function refreshIndexes() {

    if (!indexlist.length) {
        return setTimeout(refreshIndexes, 1000);
    }

    var first = indexlist.shift();

    // dispose of indexes not used in the last 10 minutes
    if(first.lastused < datetime.now - 600) {
        index[first.hash] = null;
        delete(index[first.hash]);
        first = null;
        return refreshIndexes();
    }

    indexlist.push(first);

    // wait at least 30 seconds between updates
    if(datetime.now - first.lastcheck < 30) {
        return setTimeout(refreshIndexes, 1000);
    }

    // remove any scores we locally added
    var i, len, rem;

    for(i=0, len=first.remove.length; i<len; i++) {
       rem = first.remove[i];
       first.removeHash[rem.points] -= rem.scores;
    }

    // update the index
    first.query.date = { $gt: first.lastupdated };
    first.lastupdated = datetime.now;
    db.LeaderboardScore.find(first.query).sort(first.highest ? "-points" : "points").select("points date").exec(function(error, scores) {

        if(error) {
            return setTimeout(refreshIndexes, 1000);
        }

        if(!scores.length) {
            return setTimeout(refreshIndexes, 1000);
        }

        // add the new data
        var i, len, score;
        for(i=0, len=scores.length; i<len; i++)  {
            score = scores[i];

            var newscore = { points: score.points, scores: 1, before: 0 };
            addToIndex(first, first.highest, newscore);

            if(score.date > first.lastupdated) {
                first.lastupdated = score.date;
            }
        }

        return setTimeout(refreshIndexes, 1000);
    });
}

refreshIndexes();
