var buffer = require('vinyl-buffer')
  , clean = require('gulp-clean')
  , coveralls = require('gulp-coveralls')
  , gulp = require('gulp')
  , instrument = require('gulp-instrument')
  , source = require('vinyl-source-stream')
  , spawn = require('child_process').spawn

gulp.task('coveralls', ['instrument'], function(done) {
  if (!process.env.COVERALLS_REPO_TOKEN) {
    return done(new Error("No COVERALLS_REPO_TOKEN set."));
  }

  process.env.JSCOV=1;

  var err = '';

  var mocha = spawn('node_modules/mocha/bin/mocha', [
    'test', '--reporter', 'mocha-lcov-reporter'
  ]);

  mocha.stderr.on('data', function(chunk) {
    err += chunk;
  });

  mocha.stdout
    .pipe(source('lcov.json'))
    .pipe(buffer())
    .pipe(coveralls());

  mocha.on('close', function(code) {
    if (code) {
      if (err) return done(new Error(err));

      return done(new Error(
        "Failed to send lcov data to coveralls."
      ));
    }

    done();
  });
});

gulp.task('coverage', ['instrument'], function() {
  process.env.JSCOV=1;

  return spawn('node_modules/mocha/bin/mocha', [
    'test', '--reporter', 'html-cov'
  ]).stdout
    .pipe(source('coverage.html'))
    .pipe(gulp.dest('./'));
});

gulp.task('instrument', function() {
  return gulp.src('lib/**.js')
    .pipe(instrument())
    .pipe(gulp.dest('lib-cov'));
});

gulp.task('test', function() {
  return spawn('node_modules/mocha/bin/mocha', [
    'test', '--reporter', 'spec'
  ], {
    stdio: 'inherit'
  });
});

gulp.task('clean', function() {
  return gulp.src([
    'coverage.html',
    'lib-cov',
    'npm-debug.log'
  ], {
    read: false
  })
  .pipe(clean());
});
