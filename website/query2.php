<html>
  <body>
    <h1>Query 2</h1>
    <h3>Given a date and a tower-id print the total added estimated weight of all the birds seen by a tower.</h3>
<?php
$date = $_GET["date"];
$tower_id = $_GET["tower_id"]
?>
<form action="query2.php" method="get">
Date: <input type="text" name="date" <?php echo "value=". $date;?>> <br>
Tower ID: <input type="text" name="tower_id" <?php echo "value=". $tower_id;?>> <br>
<input type="submit">
</form>
<?php
//***************************************
// Enable Errors
ini_set('error_reporting', E_ALL);
//***************************************

	require 'vendor/autoload.php';
	use Aws\DynamoDb\DynamoDbClient;

$client = DynamoDbClient::factory(array(
    'key'    => '',
    'secret' => '',
    'region' => 'us-west-2'
));


$iterator = $client->getIterator('Query', array(
    'TableName' => 'query2',
    'KeyConditions' => array(
        'date' => array(
            'AttributeValueList' => array(
                array('S' => $date)
            ),
            'ComparisonOperator' => 'EQ'
        ),
        'tower_id' => array(
            'AttributeValueList' => array(
                array('S' => $tower_id)
            ),
            'ComparisonOperator' => 'EQ'
        )
    )
));


if(iterator_count($iterator) > 0) {
    foreach ($iterator as $item) {
        echo "Weight sum: " . $item['weight_sum']['S'] . "<br>";
    } 
} else {
    echo 'No Data for date: ' . $date . ' and tower_id: ' . $tower_id;
}

?>
</body>
</html>
