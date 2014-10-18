load 'watch_tower.rb'

def data_gen(iterations)
    File.open("data.txt","a") do |fh_out|
      iterations.to_i.times do
        data_in = WatchTower.new.watch
        fh_out.puts data_in
        puts data_in
      end
    end
end

data_gen(ARGV[0])