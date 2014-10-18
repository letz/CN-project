class WatchTower

  def initialize
    @tower_id = "#{str}#{rand(10)}"
  end

  def watch
    d = date
    "T-#{@tower_id},#{d.strftime('%F')},#{d.strftime('%T')},#{bird},#{weight},#{span},#{weather}"
  end

private

  def date
    Time.now - rand(10000000)
  end

  def bird
    "B-#{str}-#{rand(100)}"
  end

  def weight
    1 + rand(4)
  end

  def span
    10 + rand(10)
  end

  def weather
    rand(3)
  end


  def init_strs
    str = 'AA'
    @init_strs ||= [].tap { |ary| ary << str.next!.clone while str.size < 3 }
  end

  def str
    init_strs.sample
  end

end
