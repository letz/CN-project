<html>
  <body>
    <h1> Query 1</h1>
    <h3>Given a date, display the tower that observed the bird with biggest wingspan in raining conditions.</h3>
<?php $date = $_GET["date"]; ?>
<form action="query1.php" method="get">
<label for="date">Date:</label>
<input type="text" id="date" <?php echo "value=". $date;?>>
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
    'key'    => 'AKIAJVD3GJD2JG7QM32A',
    'secret' => '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',
    'region' => 'us-west-2'
));

$weather = 2;


$iterator = $client->getIterator('Scan', array(
    'TableName' => 'query',
    'ScanFilter' => array(
        'date' => array(
            'AttributeValueList' => array(
                array('S' => $date)
            ),
            'ComparisonOperator' => 'EQ'
        ),
        'weather' => array(
            'AttributeValueList' => array(
                array('N' => $weather)
            ),
            'ComparisonOperator' => 'EQ'
        )
    )
));

$tower = 0;
$max_wing_span = 0;

if(iterator_count($iterator) > 0) {
    foreach ($iterator as $item) {
     if($max_wing_span < $item['wing_span']) {
         $tower = $item['tower_id']['S'];
         $max_wing_span = $item['wing_span']['N'];
     } 
   }
   echo "tower_id: " . $tower . "<br>";
   echo "wing_span: " . $max_wing_span . "<br>";
} else {
    echo 'No Data for date: ' . $date;
}

?>
</body>
</html>
