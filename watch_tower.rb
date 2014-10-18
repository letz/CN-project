class WatchTower

  def initialize
    @tower_id = "#{str}-#{rand(10)}"
  end

  def watch
    d = date
    "#{@tower_id},#{d.strftime('%F')},#{d.strftime('%T')},#{bird},#{weight},#{span},#{weather}"
  end

private

  def date
    Time.now - rand(10000000)
  end

  def bird
    "#{str}-#{rand(100)}"
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

  def str
    o = [('a'..'z'), ('A'..'Z')].map { |i| i.to_a }.flatten
    (0...10).map { o[rand(o.length)] }.join
  end

end
