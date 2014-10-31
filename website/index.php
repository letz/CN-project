<?php

$user="ist168211";
$host="db.ist.utl.pt";
$port=5432;
$password="cnproject";
$dbname = $user;

$connection = pg_connect("host=$host port=$port user=$user password=$password dbname=$dbname") or die(pg_last_error());

echo '<h1>CN Project </h1>';


$sqlquery = "SELECT * FROM q1q2 WHERE t_id = 'tower-1';";

$cursor = pg_query($sqlquery) or die('ERROR: ' . pg_last_error());

echo '<h2>Query 1 and 2 </h2>';
echo "<strong> SELECT * FROM q1q2 WHERE t_id = 'tower-1';" . "</strong><br>";
?>

<table style="width:35%">
  <tr>
    <td>Tower-ID</td>
    <td>Log Date</td>
    <td>Weather</td>
    <td>Sum Weigth</td>
    <td>Max WingSpan</td>
  </tr>
<?php
while ($rowV = pg_fetch_assoc($cursor)) {
  echo '<tr>';
  echo '<td>' . $rowV["t_id"] . '</td>';
  echo '<td>' . $rowV["log_date"] . '</td>';
  echo '<td>' . $rowV["weather"] . '</td>';
  echo '<td>' . $rowV["sum_weight"] . '</td>';
  echo '<td>' . $rowV["max_ws"] . '</td>';
  echo '</tr>';
}
?>
</table>

<?php

$sqlquery = "SELECT * FROM q3;";

$cursor = pg_query($sqlquery) or die('ERROR: ' . pg_last_error());

echo '<h2>Query 3 </h2>';
echo "<strong>SELECT * FROM q3;" . "</strong><br>";
?>

<table style="width:35%">
  <tr>
    <td>Bird-ID</td>
    <td>Last Seen At</td>
  </tr>
<?php
while ($rowV = pg_fetch_assoc($cursor)) {
  echo '<tr>';
  echo '<td>' . $rowV["b_id"] . '</td>';
  echo '<td>' . $rowV["last_seen_at"] . '</td>';
  echo '</tr>';
}
?>
</table>


<?php pg_close($connection); ?>
