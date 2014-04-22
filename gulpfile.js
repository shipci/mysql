var clean = require('gulp-clean')
  , coveralls = require('gulp-coveralls')
  , gulp = require('gulp')
  , instrument = require('gulp-instrument')
  , source = require('vinyl-source-stream')
  , spawn = require('child_process').spawn

gulp.task('coveralls', ['instrument'], function() {
  if (!process.env.TRAVIS) return;

  process.env.JSCOV=1;

  return spawn('node_modules/mocha/bin/mocha', [
    'test', '--reporter', 'json-lcov'
  ])
  .stdout.pipe(coveralls());
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
