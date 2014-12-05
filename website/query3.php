<html>
  <body>
    <h1>Query 3</h1>
    <h3>List all tagged birds that have not been observed for more than one week.</h3>
<?php
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

$one_week_ago = strtotime("-1 week") * 1000;


$iterator = $client->getIterator('Scan', array(
    'TableName' => 'query3',
    'ScanFilter' => array(
        'date' => array(
            'AttributeValueList' => array(
                array('N' => $one_week_ago)
            ),
            'ComparisonOperator' => 'LE'
        ),
        'bird_id' => array(
            'AttributeValueList' => array(
                array('S' => "0")
            ),
            'ComparisonOperator' => 'NE'
        )
    )
));


if(iterator_count($iterator) > 0) {
    foreach ($iterator as $item) {
	$date = date('r', $item['date']['N'] / 1000);
        echo $item['bird_id']['S'] . " - " . $date . "<br>";
    } 
} else {
    echo 'All the tagged birds were seen between last week and now';
}

?>
</body>
</html>
