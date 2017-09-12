<?php
	function evaluate_age($str_timestamp, $age_select){
		$str_gmt = substr($str_timestamp, 0, -6);
		$timestamp = strtotime($str_gmt);
		$elapsed = time() - $timestamp;

		switch ($age_select) {
			case '1_min':
				return $elapsed <= 60;
			case '10_min':
				return $elapsed <= 600;
			case '30_min':
				return $elapsed <= 1800;
			case '1_hour':
				return $elapsed <= 3600;
			case 'all':
				return true;
			default:
				return false;
		}
	}

	function scan($dir){
		if(file_exists($dir)){
			foreach(scandir($dir) as $f) {
				if(!$f || $f[0] == '.' || is_dir($dir . '/' . $f)) {
					continue; // Ignore hidden files or subfolders
				}else {
					// It is a file
					$filename = $dir . '/' . $f;
					$file_parts = pathinfo($filename);
					if (($file_parts['extension'] == "json") && (filesize($filename) > 0)){				
						$files[] = $filename;
					}
	            }
	        }
	    }
		return $files;
	}

	$config = parse_ini_file ("data.ini");
	$dir = $config['DATA_DIR'];

	$age_select = $_GET['age'];
	$response = scan($dir);

	$features = array();
	foreach ($response as $file) {
		$fn = fopen($file,"r");
		while(!feof($fn))  {
			$line = fgets($fn);
			if($line != ''){
				$json = json_decode($line, true);

				$execute = evaluate_age($json['time'], $age_select);
				if($execute){
					$features[] = array(
			        	'type' => 'Feature',
			        	'properties' => array(
			        		'id' => $json['id'],
			        		'time' => $json['time'],
			        		'text' => $json['text'],
			        		'prediction' => $json['prediction'],
			        	),
			        	'geometry' => array(
			        		'type' => 'Point', 
			        		'coordinates' => array(
			        			$json['longitude'], 
			        			$json['latitude'], 
			        			1
			        		),
			        	),
		        	);
				}
	  		}
		}

  		fclose($fn);		
    }

	$new_data = array(
    	'type' => 'FeatureCollection',
    	'features' => $features,
	);

	$final_data = json_encode($new_data, JSON_PRETTY_PRINT);
	print_r($final_data);
?>