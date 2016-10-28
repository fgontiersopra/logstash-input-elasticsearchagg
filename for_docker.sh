export PATH=$PATH:/opt/logstash/vendor/jruby/bin
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
gem install ruby
gem install bundle
gem install rake
bundle install --path opt/logstaash/vendor/cache
bundle exec rspec
