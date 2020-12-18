const redis = require("redis");
const { promisify } = require("util");

function myDB() {
  const myDB = {};

  // We have only one connection per server
  const client = redis.createClient();

  client.on("error", function (error) {
    // TODO: HANDLE ERRORS
    console.error(error);
  });

  myDB.getMovies = async function (page) {
    const pzrange = promisify(client.zrange).bind(client);
    const phgetall = promisify(client.hgetall).bind(client);

    const ids = await pzrange("movies", 0, -1);

    // console.log("Got movie ids", ids);

    // Iterate over the ids to get the details
    const promises = [];
    for (let id of ids) {
      promises.push(phgetall("movie:" + id));
    }

    const movies = await Promise.all(promises);
    // console.log("Movies details", movies);

    return movies;
  };

  myDB.createMovie = async function (movie) {
    // Convert the callback-based client.incr into a promise
    const pincr = promisify(client.incr).bind(client);
    const phmset = promisify(client.hmset).bind(client);
    const pzadd = promisify(client.zadd).bind(client);

    movie.id = await pincr("countMovieId");
    await phmset("movie:" + movie.id, movie);
    return pzadd("movies", +new Date(), movie.id);
  };

  myDB.updateMovie = async function (movie) {
    const phmset = promisify(client.hmset).bind(client);
    return phmset("movie:" + movie.id, movie);
  };

  myDB.deleteMovie = async function (movie) {
    const pdel = promisify(client.del).bind(client);
    const pzrem = promisify(client.zrem).bind(client);

    await pdel("movie:" + movie.id);
    return await pzrem("movies", movie.id);
  };

  myDB.getPeople = async function (page) {
    const pzrange = promisify(client.zrange).bind(client);
    const phgetall = promisify(client.hgetall).bind(client);

    const ids = await pzrange("people", 0, -1);

    console.log("Got people ids", ids);

    // Iterate over the ids to get the details
    const promises = [];
    for (let id of ids) {
      promises.push(phgetall("person:" + id));
    }

    const people = await Promise.all(promises);
    console.log("People details", people);

    return people;
  };

  myDB.createPerson = async function (person) {
    // Convert the callback-based client.incr into a promise
    const pincr = promisify(client.incr).bind(client);
    const phmset = promisify(client.hmset).bind(client);
    const pzadd = promisify(client.zadd).bind(client);

    person.id = await pincr("countPersonId");
    await phmset("person:" + person.id, person);
    return pzadd("people", +new Date(), person.id);
  };

  myDB.updatePerson = async function (person) {
    const phmset = promisify(client.hmset).bind(client);
    return phmset("person:" + person.id, person);
  };

  myDB.deletePerson = async function (person) {
    const pdel = promisify(client.del).bind(client);
    const pzrem = promisify(client.zrem).bind(client);

    await pdel("person:" + person.id);
    return await pzrem("people", person.id);
  };

  return myDB;
}

module.exports = myDB();
