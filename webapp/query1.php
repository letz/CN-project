<html>
  <body>
    <h1> Query 1</h1>
    <h3>Given a date, display the tower that observed the bird with biggest wingspan in raining conditions.</h3>
<?php $date = $_GET["date"]; ?>
<form action="query1.php" method="get">
Date:<input type="text" name="date" <?php echo "value=". $date;?>>
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
    'key'    => 'AKIAIVXREAQ44IAY64OA',
    'secret' => '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',
    'region' => 'us-west-2'
));



$iterator = $client->getIterator('Query', array(
    'TableName' => 'query1',
    'KeyConditions' => array(
        'date' => array(
            'AttributeValueList' => array(
                array('S' => $date)
            ),
            'ComparisonOperator' => 'EQ'
        )
    )
));

$tower = 0;
$max_wing_span = 0;

if(iterator_count($iterator) > 0) {
    foreach ($iterator as $item) {
     $tower = $item['tower_id']['S'];
     $max_wing_span = $item['max_ws']['S'];
   }
   echo "tower_id: " . $tower . "<br>";
   echo "wing_span: " . $max_wing_span . "<br>";
} else {
    echo 'No Data for date: ' . $date;
}

?>
</body>
</html>
